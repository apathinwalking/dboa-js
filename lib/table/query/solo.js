var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../../helpers.js");
var column = require('../../column/column.js');
var soloColumnQuery = require('../../column/query/solo.js');
var multColumnQuery = require('../../column/query/mult.js');

exports._getColObjs = function(tblObj){
  return exports.columnNames(tblObj)
    .then(function(results){
      return _.map(results,function(c){
        return _.assign({},tblObj,{column_name:c});
      });
    });
};

//args: either {column_name: <something>, or [{column_name: "something"},{column_name: <something>}, ...]}
exports.column = function(tblObj, args) {
  if (Array.isArray(args)) {
    var colObjs = _.map(args, function(a) {
      return _.assign({}, a, tblObj);
    });
    return column(colObjs);
  } else {
    return column(_.assign({}, args, tblObj));
  }
};

exports.fqTableName = function(tblObj) {
  return tblObj.fq_table_name;
};

exports.tableName = function(tblObj) {
  return exports.info(tblObj, {
    info_types: 'table_name'
  }).then(function(results) {
    return _.pluck(results, 'table_name')[0];
  });
};

exports.schemaName = function(tblObj) {
  return exports.info(tblObj, {
    info_types: 'table_schema'
  }).then(function(results) {
    return _.pluck(results, 'table_schema')[0];
  });
};

exports.allInfo = function(tlbObj) {
  return tblObj.knex.select('*')
    .select('column_name', tblObj.knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
    .from('information_schema.columns')
    .where(tblObj.knex.raw('table_schema || \'.\' || table_name'), tblObj.fq_table_name)
    .then(function(results) {
      return results;
    });
};

//info_types
exports.info = function(tblObj, args) {
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

exports.columnNames = function(tblObj) {
  return exports.info(tblObj, {
      info_types: ['column_name']
    })
    .then(function(results) {
      return _.pluck(results, 'column_name');
    });
};

exports.comment = function(tblObj) {
  return tblObj.knex.select(tblObj.knex.raw('\'' + tblObj.fq_table_name + '\'::regclass')
      .wrap('obj_description(', ');'))
    .then(function(results) {
      return _.pluck(results, 'obj_description')[0];
    }, function(error) {
      return null;
    });

};

//match_column_names
exports.duplicates = function(tblObj, args) {
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
exports.groupByDuplicates = function(tblObj, args) {
  return exports.duplicates(tblObj, args)
    .then(function(results) {
      var grouped = _.groupBy(results, function(r) {
        return _.values(_.pick(r, args.match_column_names));
      });
      //remove the keys
      return _.values(grouped);
    });
};

//conditions
exports.deleteWhere = function(tblObj, args) {
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
exports.deleteWhereNull = function(tblObj, args) {
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
exports.copy = function(tblObj, args) {
  return tblObj.knex.raw('select * into ' + args.new_fq_table_name + ' from ' + tblObj.fq_table_name)
    .then(function(results) {
      //TODO: check if worked
      return true;
    });
};

//column_names
exports.hasColumns = function(tblObj, args) {
  return exports.columnNames(tblObj)
    .then(function(results) {
      return _.reduce(args.column_names, function(obj, c) {
        obj[c] = _.includes(results, c);
        return obj;
      }, {});
    });
};

//args: [column_name]
exports.createPrimaryKeyIndexColumn = function(tblObj, args){
    return exports.tableName(tblObj)
        .then(function(tableName){
            args = _.assign({},{column_name:tableName + '_id'},args);
            return tblObj.knex.schema.table(tblObj.fq_table_name, function(table){
                table.bigIncrements(args.column_name);
            })
            .then(function(results){
                return true;
            },function(error){
                return error;
            });
        });

};

//args: [group_by, column_name, id_column_name]
exports.createGroupIndexColumn = function(tblObj, args){
    args = _.assign({},{column_name: args.group_by + '_idx'},args);
    return tblObj.knex.schema.table(tblObj.fq_table_name, function(table){
        table.bigInteger(args.column_name);
    })
    .then(function(results){
        var quoted_group_by = '"' + args.group_by + '"';
        var quoted_id = '"' + args.id_column_name + '"';
        var t2_quoted_group_by = 't2.' + quoted_group_by;
        var t2_quoted_id = 't2.' + quoted_id;
        var t1_quoted_id = helpers.dblQuoteFqTableName(tblObj.fq_table_name) + '.' + quoted_id;
        var query = 'UPDATE ' + helpers.dblQuoteFqTableName(tblObj.fq_table_name) + ' SET ' + quoted_group_by + '=' + t2_quoted_group_by + ' FROM (' + 'SELECT ' + quoted_group_by + ', ' + quoted_id + ', ' + 'DENSE_RANK() OVER(ORDER BY ' + quoted_group_by + ') ' + args.column_name + ' FROM ' + helpers.dblQuoteFqTableName(tblObj.fq_table_name) + ' ) AS t2 WHERE '
        + t1_quoted_id + '=' + t2_quoted_id;
        console.log(query);
        return tblObj.knex.raw(query)
            .then(function(results){
                console.log(results);
                return results.rows;
            });
    },function(error){return error;})
    .then(function(results){
        return true;
    },function(error){return error;});
};
