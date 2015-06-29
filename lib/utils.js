var _ = require('lodash');
var Promise = require('bluebird');

exports.unionOn = function(knex,columnPairs,tables,src_column_name,fq_table_name){
    knex.createTable(fq_table_name)
        .as(function(){
            var knex = this;
            _.forEach(columnPairs,function(c){
                knex.select(c.columns[0].as(c.column_name))
            });
            knex.select(knex.raw(tables[0].src_value));
            knex.union(function(){
                var knex = this;
                _.forEach(columnPairs,function(c){
                    knex.select(c.columns[1].as(c.column_name))
                })
            });
        })
};