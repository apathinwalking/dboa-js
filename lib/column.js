var Promise = require('bluebird');
var _ = require('lodash');
var datatypes = require('./data-types.js');

module.exports = function(opts){
    var tableId = opts.tableId;
    var columnName = opts.columnName;
    //numeric data_types
    var numeric = datatypes.numeric;



    //count the number of null values
    var nullCount = function(){
        return knex.count(columnName)
            .from(tableId)
            .whereNull(columnName)
            .then(function(results){
                return parseInt(
                    _.pluck(results,'count')[0]
                );
            });
    };

    //count the number of rows
    var rowCount = function(){
        return knex.count(columnName)
            .from(tableId)
            .then(function(results){
                return parseInt(
                    _.pluck(results,'count')[0]
                );
            });
    };

    //count the number of occurences of a value
    var valueCount = function(value){
        return knex.count(columnName)
            .from(tableId)
            .where(columnName,value)
            .then(function(results){
                return parseInt(
                    _.pluck(results,'count')[0]
                );
            });
    };

    var instance = {};

    //returns the average of rows
    instance._avg = function(){
        return knex.avg(columnName)
            .from(tableId)
            .then(function(results){
                return _.pluck(results,'avg')[0];
            });
    };

    //gets the avg, does a type check first returns null if typecheck fails
    instance.avg = function(){
        return instance.typeIn(self._numeric)
            .then(function(results){
                if(results === true){
                    return instance._avg();
                }
                else{
                    return null;
                }
            });
    };

    //count the rows, occurences of a value, or number of nulls
    instance.count = function(value){
        if(value === undefined){
            return rowCount(tableId,columnName);
        }
        else if(value === null){
            return nullCount(tableId,columnName);
        }
        else{
            return valueCount(tableId,columnName);
        }
    };

    //compares whether both column's data types are in a set of data_types
    instance.compareTypeIn = function(column,types){
        return Promise.all([
            instance.typeIn(types),
            column.typeIn(types)
        ]).spread(function(selfCheck,columnCheck){
            if(selfCheck === false || columnCheck === false){
                return false;
            }
            else{
                return true;
            }
        });
    };

    //gets the distinct values in the column
    instance.distinct = function(){
        return knex.distinct(columnName)
            .from(tableId)
            .pluck(columnName)
            .then(function(results){
                return results;
            });
    };

    //count the number of distinct values in the column (faster than getting distinct values)
    instance.distinctCount = function(){
        return knex.count()
            .from(function(){
                this.distinct(self.columnName)
                    .from(tableId)
                    .as('tmp')
            })
            .then(function(results){
                return parseInt(
                    _.pluck(results,'count')[0]
                );
            });
    };

    //gets the ratio of the number of distinct values to the number of columns
    instance.distinctRatio = function(){
        //TODO: allow options for ignoring null and whatnot
        return Promise.all([
            instance.distinctCount(),
            rowCount()
        ]).spread(function(distinctCount,rowCount){
            return (distinctCount/rowCount);
        });
    };

    //queries columns from information_schema.columns
    instance.info = function(infoTypes){
        return knex.select(infoTypes)
            .select('column_name',knex.raw('table_schema || \'.\' || table_name as table_id'))
            .from('information_schema.columns')
            .where('column_name',columnName)
            .then(function(results){
                return _.map(results,function(r){
                    return _.pick(r,infoTypes);
                });
            });
    };

    //just return the column name;
    instance.name = function(){
        return columnName;
    }

    //gets the minimum value
    instance.min = function(){
        return knex.min(columnName)
            .from(tableId)
            .then(function(results){
                return _.pluck(results,'min')[0];
            });
    };

    //gets the maximum value
    instance.max = function(){
        return knex.max(columnName)
            .from(tableId)
            .then(function(results){
                return _.pluck(results,'max')[0];
            });
    };

    //gets the sum of values in the column
    instance._sum = function(){
        //TODO: what about null?
        return knex.sum(columnName)
            .from(tableId)
            .then(function(results){
                return _.pluck(results,'sum')[0];
            });
    };

    //gets the sum, does a type check first returns null if typecheck fails
    instance.sum = function(){
        return instance.typeIn(numeric)
            .then(function(results){
                if(results === true){
                    return instance._sum();
                }
                else{
                    return null;
                }
            });
    };

    //just return tableId
    instance.table = function(){
        return tableId;
    };

    //gets the data_type of the column
    instance.type = function(){
        return instance.info('data_type')
            .then(function(results){
                return _.pluck(results,'data_type')[0];
            });
    };

    //tells you whether the data_type is in a list of types
    instance.typeIn = function(types){
        return instance.type()
            .then(function(results){
                return _.includes(types,results)
            });
    };

    return instance;
};