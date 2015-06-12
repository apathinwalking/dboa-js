var Q = require('q');
var _ = require('lodash');

exports.getTableNames = function(knex,schemaNames){
    "use strict";
    var deferred = Q.defer();
    knex.select('table_name','table_schema')
        .from('information_schema.tables')
        .whereIn('table_schema',schemaNames)
        .then(function(rows){
            deferred.resolve(rows);
        });
    return deferred.promise;
};

exports.getTableIds = function(knex,schemaNames){
    "use strict";
    var deferred = Q.defer();
    knex.select(knex.raw('table_schema || \'.\' || table_name as table_id'),'table_name','table_schema')
        .from('information_schema.tables')
        .whereIn('table_schema',schemaNames)
        .then(function(rows){
            deferred.resolve(rows);
        });
    return deferred.promise;
};