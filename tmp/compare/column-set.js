var Promise = require('bluebird');
var _ = require('lodash');
var datatypes = require('../data-types.js');
var matcher = require('../matcher.js');
var columnCompare = require('./column.js');
var helpers = require('../helpers.js');

module.exports = function(columnSet1,columnSet2){
    var columnSet1 = columnSet1;
    var columnSet2 = columnSet2;
    var paired = helpers.pairColumns(columnSet1,columnSet2);


    var wrapper = function(functionName,opts){
        var promises = _.map(paired,function(columnPair){
            return columnCompare(columnPair.column1,columnPair.column2)
                [functionName](opts)
                .then(function(results){
                    console.log(columnPair.column2.name() + " " + columnPair.column1.name());
                    console.log(results);
                    return {columns:columnPair,value:results}
                });
        });
        return Promise.all(promises)
            .then(function(results){
                console.log(results);
                return matcher(results,columnSet1,columnSet2);
            })
    };

    var init = {};

    init.name = function(){
        var results =  _.map(paired,function(columnPair){
            return {columns:columnPair,value:columnCompare(columnPair.column1,columnPair.column2).name()};
        });
        return matcher(results,columnSet1,columnSet2);
    };

    init.avg = function(){
        return wrapper("avg");
    };

    init.count = function(){
        return wrapper("count");
    };

    init.type = function(){
        return wrapper("type");
    };

    init.sum = function(){
        return wrapper("sum");
    };

    init.nameEditDistance = function(){
        return wrapper("nameEditDistance")
    };





    return init;
};
