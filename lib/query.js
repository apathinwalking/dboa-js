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

exports.column.valueCount = function(knex,fq_table_name,column_name,value){
    return knex.count(column_name)
        .from(fq_table_name)
        .where(column_name,value)
        .then(function(results){
            return parseInt(
                _.pluck(results,'count')[0]
            );
        });
};

exports.column.info = function(knex,fq_table_name,column_name,info_types){
    return knex.select(info_types)
        .select('column_name',knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
        .from('information_schema.columns')
        .where('column_name',column_name)
        .where(knex.raw('table_schema || \'.\' || table_name'),fq_table_name)
        .then(function(results){
            return _.map(results,function(r){
                return _.pick(r,info_types);
            })[0];
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

exports.column.typeIn = function(knex,fq_table_name,column_name,data_types){
    return exports.dataType(knex,fq_table_name,column_name)
        .then(function(results){
            return _.includes(data_types,results)
        });
};

exports.column.min = function(knex,fq_table_name,column_name){
    return knex.min(column_name)
        .from(fq_table_name)
        .then(function(results){
            return _.pluck(results,'min')[0];
        });
};

exports.column.max = function(knex,fq_table_name,column_name){
    return knex.max(column_name)
        .from(fq_table_name)
        .then(function(results){
            return _.pluck(results,'max')[0];
        });
};

exports.column.ratio = function(knex,fq_table_name,column_name,value){
    return Promise.all([
        exports.column.rowCount(knex,fq_table_name,column_name),
        exports.column.valueCount(knex,fq_table_name,column_name,value)
    ]).spread(function(rowCount,valueCount){
        return valueCount/rowCount;
    });
};

exports.column.sum = function(knex,fq_table_name,column_name){
    return knex.sum(column_name)
        .from(fq_table_name)
        .then(function(results){
            return _.pluck(results,'sum')[0];
        },function(error){
            return null;
        });
};

exports.column.avg = function(knex,fq_table_name,column_name){
    return knex.avg(column_name)
        .from(fq_table_name)
        .then(function(results){
            return _.pluck(results,'avg')[0];
        },function(error){
            return null;
        });
};

exports.column.distinct = function(knex,fq_table_name,column_name){
    return knex.distinct(column_name)
        .from(fq_table_name)
        .pluck(column_name)
        .then(function(results){
            return results;
        });
};

exports.column.distinctCount = function(knex,fq_table_name,column_name){
    return knex.count()
        .from(function(){
            this.distinct(column_name)
                .from(fq_table_name)
                .as('tmp')
        })
        .then(function(results){
            return parseInt(
                _.pluck(results,'count')[0]
            );
        });
};

exports.column.distinctRatio = function(knex,fq_table_name,column_name){
    return Promise.all([
        exports.column.distinctCount(knex,fq_table_name,column_name),
        exports.column.rowCount(knex,fq_table_name,column_nmae)
    ]).spread(function(distinctCount,rowCount){
        return (distinctCount/rowCount);
    });
};

exports.column.comment = function(knex,fq_table_name,column_name){
    return exports.column.info(knex,fq_table_name,column_name,['ordinal_position'])
        .then(function(results){
            return knex.select(knex.raw('\'' + fq_table_name + '\'::regclass, ' + results.ordinal_position)
                .wrap('col_description(',');'))
                .then(function(results){
                    return _.pluck(results,'col_description')[0]
                },function(error){
                    return null;
                });
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

exports.table.info = function(knex,fq_table_name,info_types){
    return knex.select(info_types)
        .select('column_name',knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
        .from('information_schema.columns')
        .where(knex.raw('table_schema || \'.\' || table_name'),fq_table_name)
        .then(function(results){
            return _.map(results,function(r){
                return _.pick(r,info_types);
            });
        })
};

exports.table.columnNames = function(knex,fq_table_name){
    return exports.table.info(knex,fq_table_name,['column_name'])
        .then(function(results){
            return _.pluck(results,'column_name');
        });
};

exports.table.comment = function(knex,fq_table_name){
    return knex.select(knex.raw('\'' + fq_table_name + '\'::regclass')
        .wrap('obj_description(',');'))
        .then(function(results){
            return _.pluck(results,'obj_description')[0];
        },function(error){
            return null;
        });

};

exports.utils = {};


//TODO: fix this mess
exports.utils._unionOnQuery = function(knex,src_column_name,fq_table_name1,fq_table_name2,src_value1,src_value2,matched_columns1,matched_columns2,unmatched_column_names1,unmatched_column_names2,matched_as){
    var query1 = knex
        .from(fq_table_name1);

    var query2 = knex
        .from(fq_table_name2);

    _.forEach(_.range(0,matched_as.length),function(i){
        if(matched_columns1[i].data_type !== matched_columns2[i].data_type){
            query1 = query1.select(function(){
                this.select(function(){
                    this.select(knex.raw("CAST( " + matched_columns1[i].column_name + " AS varchar )"));
                })
                    .as(matched_as[i])
            });
            query2 = query2.select(function(){
                this.select(function(){
                    this.select(knex.raw("CAST( " + matched_columns2[i].column_name + " AS varchar )"));
                })
                    .as(matched_as[i])
            });
        }
        else{
            query1 = query1.select(function(){
                this.select(matched_columns1[i].column_name)
                    .as(matched_as[i])
            });
            query2 = query2.select(function(){
                this.select(matched_columns2[i].column_name)
                    .as(matched_as[i])
            });
        }
    });

    _.forEach(unmatched_column_names1,function(u){
        query1 = query1.select(knex.raw("\"" + u + "\""));
        query2 = query2.select(helpers.nullColumn(knex,u));
    });

    _.forEach(unmatched_column_names2,function(u){
        query1 = query1.select(helpers.nullColumn(knex,u));
        query2 = query2.select(knex.raw("\"" + u + "\""));
    });

    //to allow union on self
    if(src_value1 !== null){
        var query1 = query1.select(helpers.identityColumn(knex,src_value1,src_column_name))
    }
    else{
        var query1 = query1.select(src_column_name)
    }

    var query2 = query2.select(helpers.identityColumn(knex,src_value2,src_column_name));

    return query1.union(query2);
};

exports.utils.unionOn = function(knex,src_column_name,fq_table_name1,fq_table_name2,src_value1,src_value2,matched_columns1,matched_columns2,unmatched_column_names1,unmatched_column_names2,matched_as){
    return exports.utils._unionOnQuery
        .then(function(results){
            return results;
        });
};

exports.utils.createUnionTable = function(knex,new_fq_table_name,src_column_name,fq_table_name1,fq_table_name2,src_value1,src_value2,matched_columns1,matched_columns2,unmatched_column_names1,unmatched_column_names2,matched_as){
    var union = exports.utils._unionOnQuery(knex,src_column_name,fq_table_name1,fq_table_name2,src_value1,src_value2,matched_columns1,matched_columns2,unmatched_column_names1,unmatched_column_names2,matched_as)
    return knex.raw(union).wrap("CREATE TEMP TABLE tmp1 AS (", ")")
        .then(function(){
            return knex.schema.dropTableIfExists(new_fq_table_name);
        })
        .then(function(){
            return knex.raw(knex.select().from('tmp1'))
                .wrap("CREATE TABLE " + new_fq_table_name + " AS (", ")")
        })
        .then(function(results){
            return results;
        });
};