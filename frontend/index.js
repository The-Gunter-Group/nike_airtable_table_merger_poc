/**
 * Summary. (use period)
 *
 * Description. (use period)
 *
 * @link   URL
 * @file   This files defines the MyClass class.
 * @author AuthorName.
 * @since  x.x.x
 */

 import {useBase, useRecords,  initializeBlock} from '@airtable/blocks/ui';
 import {base} from '@airtable/blocks';
 import React from "react";
 import alasql from 'alasql';
 var _ = require('lodash');
 
 
 function joinTables(left_table, right_table, leftKey, rightKey, allFields) {
 /**
  * Summary. (use period)
  *
  * Description. (use period)
  *
  * @since      x.x.x
  * @deprecated x.x.x Use new_function_name() instead.
  * @access     private
  *
  * @class
  * @augments parent
  * @mixes    mixin
  * 
  * @alias    realName
  * @memberof namespace
  *
  * @see  Function/class relied on
  * @link URL
  * @global
  *
  * @fires   eventName
  * @fires   className#eventName
  * @listens event:eventName
  * @listens className~event:eventName
  *
  * @param {type}   var           Description.
  * @param {type}   [var]         Description of optional variable.
  * @param {type}   [var=default] Description of optional variable with default variable.
  * @param {Object} objectVar     Description.
  * @param {type}   objectVar.key Description of a key in the objectVar parameter.
  *
  * @yield {type} Yielded value description.
  *
  * @return {type} Return value description.
  */    
     var res = alasql(`SELECT * FROM ? left_table \
         RIGHT JOIN ? right_table ON left_table.[${leftKey}] = right_table.[${rightKey}]`,[left_table, right_table]);
     
     res.forEach(function(row){
         try {
             delete row._rightjoin;
             allFields.forEach(function(field){
                 row[field.name] = row[field.name] || null
             })
         } catch(err) {
             //not annotated
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
 