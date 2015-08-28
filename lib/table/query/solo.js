var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../../helpers.js");
var columnCreate = require('../../column/column.js');
var soloColumnQuery = require('../../column/query/solo.js');
var multColumnQuery = require('../../column/query/mult.js');
var rawQuery = require('../../raw.js');

exports._getColObjs = function(tblObj) {
    return exports.columnNames(tblObj)
        .then(function(results) {
            return _.map(results, function(c) {
                return _.assign({}, tblObj, {
                    column_name: c
                });
            });
        });
};

//args: either {column_name: <something>, or [{column_name: "something"},{column_name: <something>}, ...]}
exports.column = function(tblObj, args) {
    if (Array.isArray(args)) {
        var colObjs = _.map(args, function(a) {
            return _.assign({}, a, tblObj);
        });
        return columnCreate(colObjs);
    } else {
        return columnCreate(_.assign({}, args, tblObj));
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

exports.allInfo = function(tblObj) {
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

//args: ['column_name', 'data_type']
exports.createColumn = function(tblObj, args) {
    return tblObj.knex.schema.table(tblObj.fq_table_name, function(table) {
        table.specificType(args.column_name, args.data_type);
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
exports.createPrimaryKeyIndexColumn = function(tblObj, args) {
    return exports.tableName(tblObj)
        .then(function(tableName) {
            args = _.assign({}, {
                column_name: tableName + '_id'
            }, args);
            return tblObj.knex.schema.table(tblObj.fq_table_name, function(table) {
                    table.bigIncrements(args.column_name);
                })
                .then(function(results) {
                    return true;
                }, function(error) {
                    return error;
                });
        });

};

//args: [partition_column_name, partitioning_column_name]
exports.createPartitionColumn = function(tblObj, args) {
    args = _.assign({}, {
        partition_column_name: args.partitioning_column_name + '_part'
    }, args);
    return exports.createColumn(tblObj, {
            column_name: args.partition_column_name,
            data_type: 'bigint'
        })
        .then(function() {
            var query = rawQuery.updatePartitionColumn({
                fq_table_name: tblObj.fq_table_name,
                partition_column_name: args.partition_column_name,
                partitioning_column_name: args.partitioning_column_name
            });
            return tblObj.knex.raw(query);
        })
        .then(function() {
            return true;
        }, function(error){
            return error;
        });
};

//args: column_names, aggregate_column_name
exports.createAggregateColumnJson = function(tblObj, args){
    args = _.assign({},{aggregate_column_name:args.column_names.join('_')},args);
    return exports.createColumn(tblObj, {
        column_name: args.aggregate_column_name,
        data_type: 'json'
    })
    .then(function(){
        var subQuery = rawQuery.colsToJson({fq_table_name:tblObj.fq_table_name,column_names:args.column_names});
        return tblObj.knex(tblObj.fq_table_name).update(args.aggregate_column_name,tblObj.knex.raw(subQuery));
    })
    .then(function(){
        return true;
    });
};

//args: column_names, aggregate_column_name
exports.createAggregateColumn = function(tblObj, args){
    args = _.assign({},{aggregate_column_name:args.column_names.join('_')},args);
    return exports.createColumn(tblObj, {
        column_name: args.aggregate_column_name,
        data_type: 'text[]'
    })
    .then(function(){
        var subQuery = rawQuery.colsToArray({fq_table_name:tblObj.fq_table_name,column_names:args.column_names});
        console.log(subQuery);
        return tblObj.knex(tblObj.fq_table_name).update(args.aggregate_column_name,tblObj.knex.raw(subQuery));
    })
    .then(function(){
        return true;
    });
};

//flatten along a partition, create new table
//args: partition_column_name, fq_table_name, primary_key_column_name
exports.createTableFromFlatPartition = function(tblObj, args){
    args = _.assign({},{fq_table_name:tblObj.fq_table_name + '_flat'},args);
    var subQuery = tblObj.knex.distinct(args.partitioning_column_name).from(tblObj.fq_table_name);
    var query = tblObj.knex.raw(subQuery.toString()).wrap('CREATE TABLE ' + helpers.quoteDotSep(args.fq_table_name) + " AS (",")");
    return query.then(function(results){
        var newTableObj = {fq_table_name:args.fq_table_name}
        var newTableObjArgs = {column_name:args.primary_key_column_name};
        return exports.createPrimaryKeyIndexColumn(newTableObj,newTableObjArgs);
    });
};
