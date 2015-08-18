var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");
var column = require("../column/column.js");
var columnQuery = require("../column/query.js");
exports.table = {};

exports.table._getColObjs = function(tblObj){
  return exports.table.columnNames(tblObj)
    .then(function(results){
      return _.map(results,function(c){
        return _.assign({},tblObj,{column_name:c});
      });
    });
};

//args: either {column_name: <something>, or [{column_name: "something"},{column_name: <something>}, ...]}
exports.table.column = function(tblObj, args) {
  if (Array.isArray(args)) {
    var colObjs = _.map(args, function(a) {
      return _.assign({}, a, tblObj);
    });
    return column(colObjs);
  } else {
    return column(_.assign({}, args, tblObj));
  }
};

exports.table.fqTableName = function(tblObj) {
  return tblObj.fq_table_name;
};

exports.table.tableName = function(tblObj) {
  return exports.table.info(tblObj, {
    info_types: 'table_name'
  }).then(function(results) {
    return _.pluck(results, 'table_name')[0];
  });
};

exports.table.schemaName = function(tblObj) {
  return exports.table.info(tblObj, {
    info_types: 'table_schema'
  }).then(function(results) {
    return _.pluck(results, 'table_schema')[0];
  });
};

exports.table.allInfo = function(tlbObj) {
  return tblObj.knex.select('*')
    .select('column_name', tblObj.knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
    .from('information_schema.columns')
    .where(tblObj.knex.raw('table_schema || \'.\' || table_name'), tblObj.fq_table_name)
    .then(function(results) {
      return results;
    });
};

//info_types
exports.table.info = function(tblObj, args) {
  return tblObj.knex.select(args.info_types)
    .select('column_name', tblObj.knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
    .from('information_schema.columns')
    .where(tblObj.knex.raw('table_schema || \'.\' || table_name'), tblObj.fq_table_name)
    .then(function(results) {
      return _.map(results, function(r) {
        return _.pick(r, args.info_types);
      });
    });
};

exports.table.columnNames = function(tblObj) {
  return exports.table.info(tblObj, {
      info_types: ['column_name']
    })
    .then(function(results) {
      return _.pluck(results, 'column_name');
    });
};

exports.table.comment = function(tblObj) {
  return tblObj.knex.select(tblObj.knex.raw('\'' + tblObj.fq_table_name + '\'::regclass')
      .wrap('obj_description(', ');'))
    .then(function(results) {
      return _.pluck(results, 'obj_description')[0];
    }, function(error) {
      return null;
    });

};

//match_column_names
exports.table.duplicates = function(tblObj, args) {
  var query_str = tblObj.knex.select().from(tblObj.fq_table_name).toString() + " as \"outer\" where (" + tblObj.knex.count().from(tblObj.fq_table_name).toString() + " as \"inner\" ";
  query_str += "where (" + helpers.parseTableColumn('inner', args.match_column_names[0]) + " = " + helpers.parseTableColumn('outer', args.match_column_names[0]) +
    " or ( " + helpers.parseTableColumn('inner', args.match_column_names[0]) + " is null and " + helpers.parseTableColumn('outer', args.match_column_names[0]) + " is null ) ) ";
  _.forEach(_.rest(args.match_column_names), function(m) {
    query_str += "and (" + helpers.parseTableColumn('inner', m) + " = " + helpers.parseTableColumn('outer', m) + " or ( " + helpers.parseTableColumn('inner', m) + " is null and " + helpers.parseTableColumn('outer', m) + " is null ) ) ";
  });
  query_str += ") > 1";
  return tblObj.knex.raw(query_str).then(function(results) {
    return results.rows;
  });
};


//match_column_names
exports.table.groupByDuplicates = function(tblObj, args) {
  return exports.table.duplicates(tblObj, args)
    .then(function(results) {
      var grouped = _.groupBy(results, function(r) {
        return _.values(_.pick(r, args.match_column_names));
      });
      //remove the keys
      return _.values(grouped);
    });
};

//conditions
exports.table.deleteWhere = function(tblObj, args) {
  var query = tblObj.knex(tblObj.fq_table_name);
  query = query.where(args.conditions[0]);
  _.forEach(_.rest(args.conditions), function(a) {
    query = query.orWhere(a);
  });
  return query.del()
    .then(function(results) {
      return results;
    });
};

//conditions
exports.table.deleteWhereNull = function(tblObj, args) {
  var query = tblObj.knex(tblObj.fq_table_name);
  query = query.whereNull(args.conditions[0]);
  _.forEach(_.rest(args.conditions), function(a) {
    query = query.orWhereNull(a);
  });
  return query.del()
    .then(function(results) {
      return results;
    });
};

//create a copy of a table, new_fq_table_name
exports.table.copy = function(tblObj, args) {
  return tblObj.knex.raw('select * into ' + args.new_fq_table_name + ' from ' + tblObj.fq_table_name)
    .then(function(results) {
      //TODO: check if worked
      return true;
    });
};

//column_names
exports.table.hasColumns = function(tblObj, args) {
  return exports.table.columnNames(tblObj)
    .then(function(results) {
      return _.reduce(args.column_names, function(obj, c) {
        obj[c] = _.includes(results, c);
        return obj;
      }, {});
    });
};

exports.tables = {};

//args: either {column_name: <something>, or [{column_name: "something"},{column_name: <something>}, ...]}
exports.tables.column = function(tblObjs, args) {
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

exports.tables.columnNameUnion = function(tableObjs){
  var promises = _.map(tableObjs,function(t){
    return exports.table.columnNames(t);
  });
  return Promise.all(promises)
    .then(function(results){
      return _.spread(_.union)(results);
    });
};

exports.tables.columnNameIntersection = function(tableObjs){
  var promises = _.map(tableObjs,function(t){
    return exports.table.columnNames(t);
  });
  return Promise.all(promises)
    .then(function(results){
      return _.spread(_.intersection)(results);
    });
};

//for each column name in the union of all column names in the tableObjs, tells you if that column exists in that table
exports.tables.columnNameDistribution = function(tableObjs){
  return exports.tables.columnNameUnion(tableObjs)
    .then(function(results){
      var promises = _.map(tableObjs, function(t){
        return exports.table.hasColumns(t,{'column_names':results});
      });
      return Promise.all(promises);
    });
};

//tells you that for each column in the union of all columns in tableObjs, whether the data types match across tables
exports.tables.getColumnCompatibleDataTypes = function(tableObjs){
  var data = {};
  return exports.tables.columnNameDistribution(tableObjs)
    .then(function(results){
      data.columnNameDistribution = results;
      return exports.tables.columnNameUnion(tableObjs);
    })
    .then(function(results){
      data.columnNameUnion = results;
      var promises = _.map(tableObjs, function(t){
        return exports.table._getColObjs(t);
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
        return columnQuery.columns.getCompatibleDataTypes(a);
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
exports.tables._unionOnAllColumnsRawQuery = function(tableObjs,args){
  args = _.assign({},{'source_column_name':'source','source_values':_.pluck(tableObjs,'fq_table_name')},args); //assign defaults
  var promises = [
    exports.tables.columnNameDistribution(tableObjs),
    Promise.all(_.map(tableObjs,exports.table._getColObjs)),
    exports.tables.columnNameUnion(tableObjs),
    exports.tables.getColumnCompatibleDataTypes(tableObjs)
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
exports.tables.unionOnAllColumns = function(tableObjs,args){
  var knex = tableObjs[0].knex;
  return exports.tables._unionOnAllColumnsRawQuery(tableObjs,args)
    .then(function(query){
      return knex.raw(query);
    })
    .then(function(results){
      return results.rows;
    });
}

//args: source_values, source_column_name, fq_table_name
exports.tables.createTableUnionOnAllColumns = function(tableObjs,args){
  var knex = tableObjs[0].knex;
  return exports.tables._unionOnAllColumnsRawQuery(tableObjs,args)
    .then(function(query){
      var createQuery = 'CREATE TABLE ' + args.fq_table_name + ' AS ' + query;
      return knex.raw(createQuery);
    })
    .then(function(results){
      return results.rows;
    });
}
