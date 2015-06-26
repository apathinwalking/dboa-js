var Promise = require('bluebird');
var _ = require('lodash');
var datatypes = require('../data-types.js');

module.exports = function(column1,column2){
    var column1 = column1;
    var column2 = column2;
    var numeric = datatypes.numeric;

    var instance = {};

    instance._avg = function(){
        return Promise.all([
            column1._avg(),
            column2._avg()
        ]).spread(function(avg1,avg2){
            return Math.abs((avg1-avg2));
        });
    };

    instance.avg = function(){
        instance.typeIn(numeric)
            .then(function(results){
                if(results === false){
                    return null;
                }
                else{
                    return instance_.avg();
                }
            });
    };

    instance.typeIn = function(types){
        return Promise.all([
            column1.typeIn(types),
            column2.typeIn(types)
        ]).spread(function(check1,check2){
            if(check1 === false || check2 === false){
                return false;
            }
            else{
                return true;
            }
        });
    };

    return instance;
};

