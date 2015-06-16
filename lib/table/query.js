var Q = require('q');
var _ = require('lodash');
var help = require('./../helpers.js');
var queryColumn = require('./../column/query.js');

exports.getColumnNames = function(knex,tableIds){
    //TODO: PARSE WITH WRAPPER
    "use strict";
    var deferred = Q.defer();
    knex.select('column_name',knex.raw('table_schema || \'.\' || table_name as table_id'))
        .from('information_schema.columns')
        .whereIn(knex.raw('table_schema || \'.\' || table_name'),tableIds)
        .then(function(rows){
            deferred.resolve(rows);
        });
    return deferred.promise;
};

exports.getColumnTypes = function(knex,tableIds){
    //TODO: PARSE WITH WRAPPER
    "use strict";
    var deferred = Q.defer();
    knex.select('column_name','data_type',knex.raw('table_schema || \'.\' || table_name as table_id'))
        .from('information_schema.columns')
        .whereIn(knex.raw('table_schema || \'.\' || table_name'),tableIds)
        .then(function(rows){
            deferred.resolve(rows);
        });
    return deferred.promise;
};

exports.getDistinctColumnTypes = function(knex,tableIds){
    "use strict";
    var deferred = Q.defer();
    this.getColumnTypes(knex,tableIds)
        .then(function(rows){
            deferred.resolve(help.mergeOn(rows,'data_type'));
        });
    return deferred.promise;
};

exports.getDistinctColumnValues = function(knex,tableIds){
    "use strict";
    var deferred = Q.defer();
    this.getColumnNames(knex,tableIds)
        .then(function(rows){
            var funcs = _.map(rows,function(r){
                return(queryColumn.getDistinct(knex, r.table_id,[r.column_name]));
            });
            return Q.all(funcs);
        })
        .then(function(rows){
            deferred.resolve(_.flatten(rows));
        });
    return deferred.promise;
};