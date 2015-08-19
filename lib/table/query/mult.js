var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../../helpers.js");
var column = require('../../column/column.js');
var soloColumnQuery = require('../../column/query/solo.js');
var multColumnQuery = require('../../column/query/mult.js');
var soloTableQuery = require('./solo.js');

//args: either {column_name: <something>, or [{column_name: "something"},{column_name: <something>}, ...]}
exports.column = function(tblObjs, args) {
  if (Array.isArray(args)) {
    //TODO: check this in a better way
    if (Array.isArray(args[0])) {
      var colObjsSets = _.map(_.range(tblObjs.length), function(i) {
        return _.map(args[i], function(a) {
          return _.assign({}, a, tblObjs[i]);
        });
      });
      return column(colObjsSets);
    } else {
      var colObjsSets2 = _.map(tblObjs, function(t) {
        return _.map(args, function(a) {
          return _.assign({}, a, t);
        });
      });
      return column(colObjsSets2);
    }
  } else {
    var colObjs = _.map(tblObjs, function(t) {
      return _.assign({}, args, t);
    });
    return column(colObjs);
  }
};

exports.columnNameUnion = function(tableObjs){
  var promises = _.map(tableObjs,function(t){
    return soloTableQuery.columnNames(t);
  });
  return Promise.all(promises)
    .then(function(results){
      return _.spread(_.union)(results);
    });
};

exports.columnNameIntersection = function(tableObjs){
  var promises = _.map(tableObjs,function(t){
    return soloTableQuery.columnNames(t);
  });
  return Promise.all(promises)
    .then(function(results){
      return _.spread(_.intersection)(results);
    });
};

//for each column name in the union of all column names in the tableObjs, tells you if that column exists in that table
exports.columnNameDistribution = function(tableObjs){
  return exports.columnNameUnion(tableObjs)
    .then(function(results){
      var promises = _.map(tableObjs, function(t){
        return soloTableQuery.hasColumns(t,{'column_names':results});
      });
      return Promise.all(promises);
    });
};

//tells you that for each column in the union of all columns in tableObjs, whether the data types match across tables
exports.getColumnCompatibleDataTypes = function(tableObjs){
  var data = {};
  return exports.columnNameDistribution(tableObjs)
    .then(function(results){
      data.columnNameDistribution = results;
      return exports.columnNameUnion(tableObjs);
    })
    .then(function(results){
      data.columnNameUnion = results;
      var promises = _.map(tableObjs, function(t){
        return soloTableQuery._getColObjs(t);
      });
      return Promise.all(promises);
    })
    .then(function(results){
      data.colObjs = results;
      var args = _.map(data.columnNameUnion,function(colName){
        return _.reduce(_.range(tableObjs.length),function(results,i){

          var fq_table_name = tableObjs[i].fq_table_name;
          var dist = data.columnNameDistribution[i];
          var objs = data.colObjs[i];
          //if the column exists in the table, return the colObj;
          if(dist[colName] === true){
            var colObj = _.findWhere(objs,{column_name:colName});
            results.push(colObj);
          }
          return results;
        },[]);
      });
      var promises = _.map(args, function(a){
        return multColumnQuery.getCompatibleDataTypes(a);
      });
      return Promise.all(promises);
    })
    .then(function(results){
      data.columnCompatibleDataTypes = results;
      return _.reduce(_.range(data.columnNameUnion.length),function(results,i){
        var colName = data.columnNameUnion[i];
        var type = data.columnCompatibleDataTypes[i];
        results[colName] = type;
        return results;
      },{});
    });
};

//args: source_values, source_column_name
exports._unionOnAllColumnsRawQuery = function(tableObjs,args){
  args = _.assign({},{'source_column_name':'source','source_values':_.pluck(tableObjs,'fq_table_name')},args); //assign defaults
  var promises = [
    exports.columnNameDistribution(tableObjs),
    Promise.all(_.map(tableObjs,soloTableQuery._getColObjs)),
    exports.columnNameUnion(tableObjs),
    exports.getColumnCompatibleDataTypes(tableObjs)
  ];
  return Promise.all(promises)
    .spread(function(columnNameDistribution,colObjs,columnNameUnion,columnCompatibleDataTypes){
      var query = _.reduce(_.range(tableObjs.length),function(query,i){
        var tableObj = tableObjs[i];
        var source_value = args.source_values[i];
        var dist = columnNameDistribution[i];
        var subQuery = 'SELECT \'' + source_value + '\' AS "' + args.source_column_name + '", ';
        subQuery = _.reduce(columnNameUnion,function(subSubQuery,columnName){
          var exists = dist[columnName];
          var type = columnCompatibleDataTypes[columnName];
          if(!exists){
            subSubQuery += 'CAST(NULL AS ' + type + ') ' + 'AS "' + columnName + '", ';
          }
          else{
            subSubQuery += 'CAST("' + columnName + '" AS ' + type + '), ';
          }
          return subSubQuery;
        },subQuery);
        subQuery = _.trimRight(subQuery,', '); //remove last comma
        subQuery += ' FROM ';
        subQuery += helpers.dblQuoteFqTableName(tableObj.fq_table_name);
        subQuery += ' UNION ALL ';
        query += subQuery;
        return query;
      },'');
      query = _.trimRight(query,' UNION ALL ');
      return query;
    });
};

//args: source_values, source_column_name
exports.unionOnAllColumns = function(tableObjs,args){
  var knex = tableObjs[0].knex;
  return exports._unionOnAllColumnsRawQuery(tableObjs,args)
    .then(function(query){
      return knex.raw(query);
    })
    .then(function(results){
      return results.rows;
    });
};

//args: source_values, source_column_name, fq_table_name
exports.createTableUnionOnAllColumns = function(tableObjs,args){
  var knex = tableObjs[0].knex;
  return exports._unionOnAllColumnsRawQuery(tableObjs,args)
    .then(function(query){
      var createQuery = 'CREATE TABLE ' + args.fq_table_name + ' AS ' + query;
      return knex.raw(createQuery);
    })
    .then(function(results){
      return true;
    },function(error){
      console.log(error);
      return false;
    });
    //TODO: return table Object
    //TODO: add foreign key mapping
};
