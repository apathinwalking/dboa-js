var _ = require('lodash');
var Promise = require('bluebird');
var helpers = require('./helpers.js');


exports.unionOn = function(knex,src_column_name,fq_table_name1,fq_table_name2,src_value1,src_value2,matched_column_names1,matched_column_names2,unmatched_column_names1,unmatched_column_names2,matched_as){
    var query1 = knex.select(helpers.identityColumn(knex,src_value1,src_column_name))
        .from(fq_table_name1);
    var query2 = knex.select(helpers.identityColumn(knex,src_value2,src_column_name))
        .from(fq_table_name2);

    _.forEach(_.range(0,matched_as.length),function(i){
        query1 = query1.select(function(){
            this.select(matched_column_names1[i])
                .as(matched_as[i])
        });
        query2 = query2.select(function(){
            this.select(matched_column_names2[i])
                .as(matched_as[i])
        });
    });

    _.forEach(unmatched_column_names1,function(u){
        query1 = query1.select(u);
        query2 = query2.select(helpers.nullColumn(knex,u));
    });

    _.forEach(unmatched_column_names2,function(u){
        query1 = query1.select(helpers.nullColumn(knex,u));
        query2 = query2.select(u);
    });

    return query1.union(query2);
};


/*dboa.knex.select(function(){
    this.select('full_name')
        .as('name')
})
    .select('last_name')
    .select(dboa.knex.raw('\'y\' src'))
    .from('mock.data2')
    .union(function(){
        this.select(function(){
            this.select('name')
                .as('name')
        })
            .select(dboa.knex.raw('null last_name'))
            .select(dboa.knex.raw('\'x\' src'))
            .from('mock.data1')
    }).limit(20)
    .then(function(results){
        console.log(results);
    });*/

/*
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
};*/
