var Q = require('q');
var _ = require('lodash');
var leven = require('leven');
var queryTable = require('./query.js');
var help = require('./../helpers.js');

exports._compareColumnNames = function(knex,tableIds,compareFunction,outputColumnName){
    "use strict";
    //TODO: Don't calculate twice!
    var deferred = Q.defer();
    queryTable.getColumnNames(knex,tableIds)
        .then(function(rows){
            var permuted = help.permuteOn(rows,'table_id');
            var compared = _.map(permuted, function(p){
                p[outputColumnName] = compareFunction(p.column_name[0], p.column_name[1]);
                return p;
            });
            deferred.resolve(compared);
        });
    return deferred.promise;
};

exports.compareColumnNamesEquality = function(knex,tableIds){
    "use strict";
    var deferred = Q.defer();
    this._compareColumnNames(knex,tableIds,function(a,b){
        if(a === b){return 1;}
        else{return 0;}
    },'equality')
        .then(function(results){
            deferred.resolve(results)
        });
    return deferred.promise;
};

exports.compareColumnNamesEditDistance = function(knex,tableIds){
    "use strict";
    var deferred = Q.defer();
    this._compareColumnNames(knex,tableIds,leven,'edit_distance')
        .then(function(results){
            deferred.resolve(results)
        });
    return deferred.promise;
};

