var Promise = require('bluebird');
var _ = require('lodash');

exports.promiseAbs = function(promise1,promise2){
    return Promise.all([
        promise1,
        promise2
    ]).spread(function(val1,val2){
        return Math.abs((val1-val2));
    });
};

exports.pairColumns = function(columnSet1,columnSet2){
    return _.reduce(columnSet1,function(results,column1){
        _.forEach(columnSet2,function(column2){
            results.push({column1:column1,column2:column2});
        });
        return results;
    },[])
};
