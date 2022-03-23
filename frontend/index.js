/**
 * Proof of concept for merging/ETL operations in Airtable. 
 *
 * This provides a proof of concept for basic ETL/merging operations between tables in a single Airtable base. Base operations are:
 * Merge/Join
 * CRUD for record Data
 * 
 * A few TODOs should be considered for this POC:
 *  - Configurability - Consideration should be given to how configurable this process can/should be:
 *      ~ UI for selecting for Left and Right Tables?
 *      ~ Dynamically selecting join keys
 *  - Error Handling:
 *      ~ For the POC, demonstrating the functionality was within scope but adequate error handling was not.
 *  - Unit Test
 *  - Rate limiting:
 *      ~ Airtable APIs rate limit writes to 50 records a request
 *      ~ Airtable APIs rate limit reads to 500 records a request
 *  - Deployment - Deployment to Airtable marketplace was outside of the scope, including use across bases
 *
 * @author The Gunter Group, attn: andeo@guntergroup.com
 * @since  3.23.2022
 */

 import {useBase, useRecords,  initializeBlock} from '@airtable/blocks/ui';
 import {base} from '@airtable/blocks';
 import React from "react";
 import alasql from 'alasql';
 var _ = require('lodash');
 
/** ****************             Table Building/Merging         **************************** */

 function joinTables(leftTable, rightTable, leftKey, rightKey, allFields) {
 /**
  * Join two tables given a join key for each.
  *
  * Basic implementation of a join between two tables. This function
  * leverages alasql for performing the join. For some record level details
  * additional information is not needed and is filtered out.
  *
  * @since      3.23.2022
  * @access     public
  *
  *
  * @param {Object}             leftTable          Left table to join.
  * @param {Object}             rightTable         Right table to join.
  * @param {string}             leftKey            Key of left table to use for joining.
  * @param {string}             rightKey           Key of right table to use for joining.
  * @param {Array.<string>}     allFields          Array of fields that should exist in the final table.
  *
  *
  * @return {Object.<string, (string|number|boolean)} Joined table.
  */    
     var res = alasql(`SELECT * FROM ? left_table \
         RIGHT JOIN ? right_table ON left_table.[${leftKey}] = right_table.[${rightKey}]`,[leftTable, rightTable]);
     
     res.forEach(function(row){
         try {
             delete row._rightjoin; // Remove fields annotated during the join
             allFields.forEach(function(field){
                 row[field.name] = row[field.name] || null
             })
         } catch(err) {
             // not annotated, ignore
         }
     })
 
     return res;
 }
 
 function formatJoinableTable(table, recordsRequired=true) {
 /**
  * Rework airtable object to create a table that can be joined.
  *
  * Airtable objects contain information that can't/doesn't need to 
  * be included in the data that gets pushed back to the tables. This 
  * function removes information that shouldn't be included in the joining
  * process.
  *
  * @since      3.23.2022
  * @access     public
  *
  *
  * @param {Object}   table             Airtable table object to transform.
  * @param {boolean}  recordsRequired   Boolean flag to include records.
  *
  *
  * @return {Array.<Object, Array>} Formatted table and array of fields.
  */  

     var tempTable = [];
     var records = useRecords(table);
     var fieldArray = [];
     records.forEach(function(record){
         var rowMap = {};
         table.fields.forEach(function(field){
             var cells = record.getCellValue(field);
             // Remove Ids from objects because Airtable IDs aren't relevant in new table
             try {
                 delete cells.id;
             } catch(err) {
                 // not an object
             }
             try {
                 cells.forEach(function(cell) {
                     delete cell.id;
                 })
             } catch(err) {
                 //do nothing for now.
             }
             fieldArray.push(field)
             rowMap[field.name] = cells
             if (recordsRequired) {
                 try {
                     rowMap['id'] = record.id
                 } catch (err) {
                     // Record is not defined.
                 }
             }
         })
         tempTable.push(rowMap)
     })
     return {tempTable, fieldArray}
 }
 
 function createTableInDB(table, name) {
 /**
  * Create Alasql table given an object and a name.
  *
  *
  * @since      3.23.2022
  * @access     public
  *
  *
  * @param {Object}   table     Object to turn into an alasql table.
  * @param {string}   name      Name to create for table.
  *
  *
  * @return {boolean}  .
  */ 

     alasql(`DROP TABLE IF EXISTS table_${name.replace(/\s/g, '')}`)
     alasql(`CREATE TABLE IF NOT EXISTS table_${name.replace(/\s/g, '')}`)
     alasql(`SELECT * INTO table_${name.replace(/\s/g, '')} FROM ?`, [table])
 }
 

 function joinTablesinDB(sourceTableName, mergedTableName) {
 /**
  * Join two tables in Alasql.
  *
  *
  * @since      3.23.2022
  * @access     public
  *
  *
  * @param {string}   sourceTableName      Source table name.
  * @param {string}   mergedTableName      Name to create for table.
  *
  *
  */ 
    alasql(`DROP TABLE IF EXISTS ${mergedTableName.replace(/\s/g, '')}`)
    alasql(`CREATE TABLE IF NOT EXISTS ${mergedTableName.replace(/\s/g, '')}`)
    alasql(`SELECT * INTO ${mergedTableName.replace(/\s/g, '')} FROM table_source RIGHT JOIN table_${sourceTableName.replace(/\s/g, '')} ON table_source.[Concept Name] = table_${sourceTableName.replace(/\s/g, '')}.[Concept Name]`)
}

async function createMergedTable(name, allFields, key) {
 /**
  * Creates a new merged table in the airtable base.
  *
  *
  * @since      3.23.2022
  * @access     public
  *
  *
  * @param {string}           name          Name of merged table.
  * @param {Array.<string>}   allFields     Array of fields to include in merged table.
  * @param {string}           key           Merge key.
  *
  *
  */     
    const tableFlag = base.getTableByNameIfExists(name);

    const field_array = []

    allFields.forEach(field => {
        const {id, name, description, type, options, ...tempArray} = field;
        const fieldMap = {'name': name, 'description': description, 'type': type, 'options': options};
        try {
            fieldMap.options.choices.forEach(choice => {
                delete choice.id;
            })
        }
        catch(err) {
            // Do nothing for now
        }
        let cleaned = Object.fromEntries(Object.entries(fieldMap).filter(([_, v])=>v!=null));
        field_array.push(cleaned);
    });
    const names = field_array.map(o => o.name)
    const filteredFieldArray = field_array.filter(({name}, index) => !names.includes(name, index+1))
    filteredFieldArray.sort((x,y)=>{ return x.name === key ? -1 : y.name === key ? 1 : 0; });

    if (base.hasPermissionToCreateTable(name, filteredFieldArray) && !tableFlag) {
        await base.createTableAsync(name, filteredFieldArray);
    }

}

 /** ****************             CRUD Operations        **************************** */
 async function insertMergedRecords(mergedTable, name) {
 /**
  * Insert Merged Records to new table.
  *
  *
  * @since      3.23.2022
  * @access     public
  *
  *
  * @param {Object}   mergedTable     Object to turn into an alasql table.
  * @param {string}   name            Name of table to write to.
  *
  *
  */ 
     const table = base.getTableByNameIfExists(name);
     if (mergedTable && mergedTable.length) {
         if (mergedTable) {
             var recordDefs = [];
             mergedTable.forEach(function(row){
                 delete row.id;
                 recordDefs.push({fields:row})
             })
             if (table.hasPermissionToCreateRecords(recordDefs)) {
                 const newRecordIds = await table.createRecordsAsync(recordDefs);
             }
         }
     }
 }
 
 async function deleteStaleRecordsFromTable(staleRecords, mergeTable, name) {
 /**
  * Delete stale records from table.
  *
  *
  * @since      3.23.2022
  * @access     public
  *
  *
  * @param {Array.<Object>}   staleRecords     Array of stale records to delete.
  * @param {Object}           mergeTable       Name of table to write to.
  * @param {string}           name            Name of table to write to.
  *
  *
  */      
     const table = base.getTableByNameIfExists(name);
     var records = formatJoinableTable(mergeTable)
     if (staleRecords && staleRecords.length) {
         var recordIDs = []
         try {
             staleRecords.forEach(function(record) {
                 var res = _.filter(records.tempTable, record)
                 recordIDs.push(res[0]['id'])
             })
         } catch (err) {
 
         }
         if (table.hasPermissionToDeleteRecords(recordIDs)) {
             const newRecordIds = await table.deleteRecordsAsync(recordIDs);
         }
     }
 }
 
 
 function checkRecordsForDups(table, merged) {
 /**
  * Check for and identify duplicate records.
  *
  * Duplicate records can be created when merging into the merged table
  * because the UniqueIDs from the source table can't be used in the destination
  * without including the ID in the view. This function ensures we won't have 
  * identical records get copied into the merge table more than once.
  *
  * @since      3.23.2022
  * @access     public
  *
  *
  * @param {Object}   table     Source table.
  * @param {Object}   merged    Merged Table.
  *
  * @return {Array.<Object>}  Array of duplicated records
  */ 
     if (!table.length) {
         var resultSet = merged
     } else {
         table.forEach(function(row){
             delete row.id; // Remove IDs so that we only check row values
         })
         merged.forEach(function(row){
             delete row.id; // Remove IDs so that we only check row values
         })
         var resultSet = _.differenceWith(merged, table, _.isEqual)
     }
     return resultSet
 }
 
 function findStaleRecords(table, merged) {
 /**
  * Check for and identify stale records.
  *
  * Users can delete records from the source or mapping table.
  * This identifies records that were removed.
  *
  * @since      3.23.2022
  * @access     public
  *
  *
  * @param {Object}   table     Source table.
  * @param {Object}   merged    Merged Table.
  *
  * @return {Array.<Object>}  Array of stale records
  */      
     if (!table.length) {
         var resultSet = merged
     } else {
         var resultSet = _.differenceWith(table, merged, _.isEqual)
     }
     return resultSet
 }

 
 
 var dbFlag = true; // GLOBAL - First time running the app should create the table, if not ignore
 
 function NikeAirtableMergerApp() {
 /**
  * Main routine for performing ETL.
  *
  *
  * @since      3.23.2022
  * @access     public
  *
  */ 
     const base = useBase();
 
     // Configured list of teams to map against source data
     const teams = ["Team A Mapping", "Team B Mapping"]
 
     const table = base.getTableByNameIfExists('Source Data');
     
     teams.forEach(function(team_table){
         const mergingTable = base.getTableByNameIfExists(team_table);
         let left_side = formatJoinableTable(table, 'source');
         let right_side = formatJoinableTable(mergingTable, team_table);
 
         if (dbFlag) {
             createTableInDB(left_side.tempTable, 'source');
             createTableInDB(right_side.tempTable, team_table);
         }
     
         const left_key = "Concept Name";
         const right_key = "Concept Name";
 
         let all_fields = _.union(left_side.fieldArray, right_side.fieldArray)
     
         if (left_side.tempTable && right_side.tempTable) {
             var merged = joinTables(left_side.tempTable, right_side.tempTable, left_key, right_key, all_fields);
             if (merged) {
                 var name = team_table + ' Merged Table';
                 joinTablesinDB( team_table, name)
                 createMergedTable(name, all_fields, left_key);
                 const mergedTable = base.getTableByNameIfExists(name);
                 if (mergedTable) {
                     var joinableTable = formatJoinableTable(mergedTable, true);
                     var unmergedRecords = checkRecordsForDups(joinableTable.tempTable, merged);
                     var staleRecords = findStaleRecords(joinableTable.tempTable, merged);
                     insertMergedRecords(unmergedRecords, name);
                     deleteStaleRecordsFromTable(staleRecords, mergedTable, name);
                 }
             }
         }
     })
     dbFlag = false;
     return <div>Hello Nike 🚀</div>;
 }
 
 initializeBlock(() => <NikeAirtableMergerApp />);
 