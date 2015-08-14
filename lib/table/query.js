var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");
var column = require("../column/column.js");
var columnQuery = require("../column/query.js");
exports.table = {};

exports.table._getColObjs = function(tblObj){
  return exports.table.getColumnNames(tblObj)
    .then(function(results){
      return _.map(column_names,function(c){
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

exports.tables.columnNameDistribution = function(tableObjs){
  return exports.tables.columnNameUnion(tableObjs)
    .then(function(results){
      var promises = _.map(tableObjs, function(t){
        return exports.table.hasColumns(t,{'column_names':results});
      });
      return Promise.all(promises);
    });
};

exports.tables.columnNamesUnionDataTypesMatch = function(tableObjs){
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
      var args = _.map() //go through all this shit!
    });
}

//args: source values
exports.tables.unionOnAllColumns = function(tableObjs,args){
  var knex = tableObjs[0].knex;
  var data = {};
  return exports.tables.columnNamesDistribution(tableObjs)
    .then(function(results){
      data.columnNamesDistribution = results;
      return exports.tables.columnNames(tableObjs);
    })
    .then(function(results){
      data.columnNames = results;
      var promises = _.map(tableObjs, function(t){
        return exports.table._getColObjs(t);
      });
      return Promise.all(promises);
    })
    .then(function(results){
      data.colObjs = results;
    });
};
