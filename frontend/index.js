/**
 * Proof of concept for merging/ETL operations in Airtable. 
 *
 * This provides a proof of concept for basic ETL/merging operations between tables in a single Airtable base. Base operations are:
 * Merge/Join
 * CRUD for record Data
 * 
 *
 * @link   URL
 * @file   This files defines the MyClass class.
 * @author The Gunter Group, attn: andeo@guntergroup.com
 * @since  3.23.2022
 */

 import {useBase, useRecords,  initializeBlock} from '@airtable/blocks/ui';
 import {base} from '@airtable/blocks';
 import React from "react";
 import alasql from 'alasql';
 var _ = require('lodash');
 
 
 function joinTables(leftTable, rightTable, leftKey, rightKey, allFields) {
 /**
  * Join two tables given a join key for each.
  *
  * Basic implementation of a join between two tables. This function
  * leverages alasql for performing the join. For some record level details
  * additional information is not needed and is filtered out
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
     var tempTable = [];
     var records = useRecords(table);
     var fieldArray = [];
     records.forEach(function(record){
         var rowMap = {};
         table.fields.forEach(function(field){
             var cells = record.getCellValue(field);
             // Remove Ids from objects because IDs aren't relevant in new table
             try {
                 delete cells.id;
             } catch(err) {
                 //not an object
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
     alasql(`DROP TABLE IF EXISTS table_${name.replace(/\s/g, '')}`)
     alasql(`CREATE TABLE IF NOT EXISTS table_${name.replace(/\s/g, '')}`)
     alasql(`SELECT * INTO table_${name.replace(/\s/g, '')} FROM ?`, [table])
     return true
 }
 
 async function insertMergedRecords(mergedTable, name) {
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
     if (!table.length) {
         var resultSet = merged
     } else {
         table.forEach(function(row){
             delete row.id;
         })
         merged.forEach(function(row){
             delete row.id;
         })
         var resultSet = _.differenceWith(merged, table, _.isEqual)
     }
     return resultSet
 }
 
 function findStaleRecords(table, merged) {
     if (!table.length) {
         var resultSet = merged
     } else {
         var resultSet = _.differenceWith(table, merged, _.isEqual)
     }
     return resultSet
 }
 
 async function createMergedTable(name, all_fields, key) {
     const tableFlag = base.getTableByNameIfExists(name);
 
     const field_array = []
 
     all_fields.forEach(field => {
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
 
 function joinTablesinDB(source_name, table_name, merged_name) {
     alasql(`DROP TABLE IF EXISTS ${merged_name.replace(/\s/g, '')}`)
     alasql(`CREATE TABLE IF NOT EXISTS ${merged_name.replace(/\s/g, '')}`)
     alasql(`SELECT * INTO ${merged_name.replace(/\s/g, '')} FROM table_source RIGHT JOIN table_${table_name.replace(/\s/g, '')} ON table_source.[Concept Name] = table_${table_name.replace(/\s/g, '')}.[Concept Name]`)
 }
 
 
 var dbFlag = true;
 
 function NikeAirtableMergerApp() {
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
                 joinTablesinDB('source', team_table, name)
                 createMergedTable(name, all_fields, left_key);
                 const mergedTable = base.getTableByNameIfExists(name);
                 if (mergedTable) {
                     var joinableTable = formatJoinableTable(mergedTable, true);
                     var unmergedRecords = checkRecordsForDups(joinableTable.tempTable, merged);
                     var staleRecords = findStaleRecords(joinableTable.tempTable, merged);
                     insertMergedRecords(unmergedRecords, name);
                     deleteStaleRecordsFromTable(staleRecords, mergedTable, name);
                     // updateRecordsInTable(staleRecords, mergedTable, name);
                 }
             }
         }
     })
     dbFlag = false;
     return <div>Hello Nike 🚀</div>;
 }
 
 initializeBlock(() => <NikeAirtableMergerApp />);
 