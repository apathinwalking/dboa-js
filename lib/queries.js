var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("./helpers.js");

exports.column = {};

exports.column.rowCount = function(knex,fq_table_name,column_name){
    return knex.count(column_name)
        .from(fq_table_name)
        .then(function(results){
            return parseInt(
                _.pluck(results,'count')[0]
            );
        });
};

exports.column.nullCount = function(knex,fq_table_name,column_name){
    return knex.count(column_name)
        .from(fq_table_name)
        .whereNull(column_name)
        .then(function(results){
            return parseInt(
                _.pluck(results,'count')[0]
            );
        })
};

exports.column.info = function(knex,fq_table_name,column_name,info_types){
    return knex.select(info_types)
        .select('column_name',knex.raw('table_schema || \'.\' || table_name as table_id'))
        .from('information_schema.columns')
        .where('column_name',column_name)
        .then(function(results){
            return _.map(results,function(r){
                return _.pick(r,info_types);
            });
        });
};

exports.column.allInfo = function(knex,fq_table_name,column_name){
    return knex.select('*')
        .select('column_name',knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
        .from('information_schema.columns')
        .where('column_name',column_name)
        .then(function(results){
            return results;
        });
};

exports.column.dataType = function(knex,fq_table_name,column_name){
    return exports.column.info(knex,fq_table_name,column_name,['data_type'])
        .then(function(results){
            return _.pluck(results,'data_type')[0]
        });
};


exports.table = {};

exports.table.allInfo = function(knex,fq_table_name){
    return knex.select('*')
        .select('column_name',knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
        .from('information_schema.columns')
        .where(knex.raw('table_schema || \'.\' || table_name'),fq_table_name)
        .then(function(results){
            return results;
        });
};