var Promise = require('bluebird');

exports.promiseAbs = function(promise1,promise2){
    return Promise.all([
        promise1,
        promise2
    ]).spread(function(val1,val2){
        return Math.abs((val1-val2));
    });
};
