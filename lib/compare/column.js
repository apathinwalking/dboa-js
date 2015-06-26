var Promise = require('bluebird');
var _ = require('lodash');
var datatypes = require('../data-types.js');
var helpers = require('../helpers.js');
var leven = require('leven');

module.exports = function(column1,column2){
    var column1 = column1;
    var column2 = column2;
    var numeric = datatypes.numeric;

    var instance = {};

    instance._avg = function(){
        return helpers.promiseAbs(column1._avg(), column2._avg())
    };

    instance.avg = function(){
        return instance.typeIn(numeric)
            .then(function(results){
                if(results === false){
                    return null;
                }
                else{
                    return instance._avg();
                }
            });
    };

    instance.count = function(value){
        return helpers.promiseAbs(column1.count(value), column2.count(value))
    };

    instance.distinctCount = function(){
        return helpers.promiseAbs(column1.distinctCount(), column2.distinctCount());
    };

    instance.distinctRatio = function(){
        return helpers.promiseAbs(column1.distinctRatio(),column2.distinctRatio());
    };

    instance.name = function(){
        if(column1.name() === column2.name()){return 1;}
        else{return 0;}
    };

    instance.nameEditDistance = function(){
        return leven(column1.name(),column2.name());
    };

    instance.min = function(){
        return helpers.promiseAbs(column1.min(),column2.min());
    };

    instance.max = function(){
        return helpers.promiseAbs(column1.max(),column2.max());
    };

    instance.ratio = function(value){
        return helpers.promiseAbs(column1.ratio(value),column2.ratio(value));
    };

    instance._sum = function(){
        return helpers.promiseAbs(column1._sum(),column2._sum());
    };

    instance.sum = function(){
        return instance.typeIn(numeric)
            .then(function(results){
                if(results === false){
                    return null;
                }
                else{
                    return instance._sum();
                }
            });
    };

    instance.type = function(){
        Promise.all([
            column1.type(),
            column2.type()
        ]).spread(function(type1,type2){
            if(type1 === type2){return 1;}
            else{return 0;}
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

