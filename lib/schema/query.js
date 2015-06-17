var Q = require('q');
var _ = require('lodash');
var help = require('./../helpers.js');

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

exports.getColumnNames = function(knex,schemaNames){
    "use strict";
    var deferred = Q.defer();
    exports.getTableIds(knex,schemaNames)
        .then(function(rows){
            var justIds = _.pluck(rows,'table_id');
            return require('../table/query.js').getColumnNames(knex,justIds);
        })
        .then(function(rows){
            deferred.resolve(rows);
        });
    return deferred.promise;
};

exports.getColumnTypes = function(knex,schemaNames){
    "use strict";
    var deferred = Q.defer();
    exports.getTableIds(knex,schemaNames)
        .then(function(rows) {
            var justIds = _.pluck(rows, 'table_id');
            return require('../table/query.js').getColumnTypes(knex,justIds);
        })
        .then(function(rows){
            deferred.resolve(rows);
        });
    return deferred.promise;
};

exports.Query = help.mapKnex(exports);