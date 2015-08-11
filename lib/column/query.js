var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");

exports.rowCount = function(knex,fq_table_name,column_name){
    return knex.count(column_name)
        .from(fq_table_name)
        .then(function(results){
            return parseInt(
                _.pluck(results,'count')[0]
            );
        });
};

exports.nullCount = function(knex,fq_table_name,column_name){
    return knex.count(column_name)
        .from(fq_table_name)
        .whereNull(column_name)
        .then(function(results){
            return parseInt(
                _.pluck(results,'count')[0]
            );
        });
};

exports.valueCount = function(knex,fq_table_name,column_name,value){
    return knex.count(column_name)
        .from(fq_table_name)
        .where(column_name,value)
        .then(function(results){
            return parseInt(
                _.pluck(results,'count')[0]
            );
        });
};

exports.info = function(knex,fq_table_name,column_name,info_types){
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

exports.allInfo = function(knex,fq_table_name,column_name){
    return knex.select('*')
        .select('column_name',knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
        .from('information_schema.columns')
        .where('column_name',column_name)
        .then(function(results){
            return results;
        });
};

exports.dataType = function(knex,fq_table_name,column_name){
    return exports.info(knex,fq_table_name,column_name,['data_type'])
        .then(function(results){
            return _.pluck(results,'data_type')[0]
        });
};

exports.typeIn = function(knex,fq_table_name,column_name,data_types){
    return exports.dataType(knex,fq_table_name,column_name)
        .then(function(results){
            return _.includes(data_types,results)
        });
};

exports.min = function(knex,fq_table_name,column_name){
    return knex.min(column_name)
        .from(fq_table_name)
        .then(function(results){
            return _.pluck(results,'min')[0];
        });
};

exports.max = function(knex,fq_table_name,column_name){
    return knex.max(column_name)
        .from(fq_table_name)
        .then(function(results){
            return _.pluck(results,'max')[0];
        });
};

exports.ratio = function(knex,fq_table_name,column_name,value){
    return Promise.all([
        exports.rowCount(knex,fq_table_name,column_name),
        exports.valueCount(knex,fq_table_name,column_name,value)
    ]).spread(function(rowCount,valueCount){
        return valueCount/rowCount;
    });
};

exports.sum = function(knex,fq_table_name,column_name){
    return knex.sum(column_name)
        .from(fq_table_name)
        .then(function(results){
            return _.pluck(results,'sum')[0];
        },function(error){
            return null;
        });
};

exports.avg = function(knex,fq_table_name,column_name){
    return knex.avg(column_name)
        .from(fq_table_name)
        .then(function(results){
            return _.pluck(results,'avg')[0];
        },function(error){
            return null;
        });
};

exports.distinct = function(knex,fq_table_name,column_name){
    return knex.distinct(column_name)
        .from(fq_table_name)
        .pluck(column_name)
        .then(function(results){
            return results;
        });
};

exports.distinctCount = function(knex,fq_table_name,column_name){
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

exports.distinctRatio = function(knex,fq_table_name,column_name){
    return Promise.all([
        exports.distinctCount(knex,fq_table_name,column_name),
        exports.rowCount(knex,fq_table_name,column_nmae)
    ]).spread(function(distinctCount,rowCount){
        return (distinctCount/rowCount);
    });
};

exports.comment = function(knex,fq_table_name,column_name){
    return exports.info(knex,fq_table_name,column_name,['ordinal_position'])
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
