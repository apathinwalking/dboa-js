var Q = require('q');
var _ = require('lodash');
var help = require('./../helpers.js');

exports.getSchemaNames = function(knex){
    "use strict";
    var deferred = Q.defer();
    knex.distinct('table_schema')
        .from('information_schema.tables')
        .then(function(rows){
            deferred.resolve(rows);
        });
    return deferred.promise;
};

exports.getTableNames = function(knex){
    "use strict";
    var deferred = Q.defer();
    exports.getSchemaNames(knex)
        .then(function(rows){
            var funcs = _.map(rows,function(r){
                return(require('../schema/query.js').getTableNames(knex,[r.table_schema]));
            });
            return(Q.all(funcs));
        })
        .then(function(rows){
            deferred.resolve(_.flatten(rows));
        });
    return deferred.promise;
};

exports.getTableIds = function(knex){
    "use strict";
    var deferred = Q.defer();
    exports.getSchemaNames(knex)
        .then(function(rows){
            var funcs = _.map(rows,function(r){
                return(require('../schema/query.js').getTableIds(knex,[r.table_schema]));
            });
            return(Q.all(funcs));
        })
        .then(function(rows){
            deferred.resolve(_.flatten(rows));
        });
    return deferred.promise;
};

exports.Query = help.mapKnex(exports);
