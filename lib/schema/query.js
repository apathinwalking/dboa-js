var Q = require('q');
var _ = require('lodash');

exports._getTableNames = function(knex,schemaNames){
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

exports._getTableIds = function(knex,schemaNames){
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

exports._getColumnNames = function(knex,schemaNames){
    "use strict";
    var deferred = Q.defer();
    exports._getTableIds(knex,schemaNames)
        .then(function(rows){
            var justIds = _.pluck(rows,'table_id');
            return require('../table/query.js').getColumnNames(knex,justIds);
        })
        .then(function(rows){
            deferred.resolve(rows);
        });
    return deferred.promise;
};

exports._getColumnTypes = function(knex,schemaNames){
    "use strict";
    var deferred = Q.defer();
    exports._getTableIds(knex,schemaNames)
        .then(function(rows) {
            var justIds = _.pluck(rows, 'table_id');
            return require('../table/query.js').getColumnTypes(knex,justIds);
        })
        .then(function(rows){
            deferred.resolve(rows);
        });
    return deferred.promise;
};

exports.Query = function(knex){
    this.knex = knex;
};

exports.Query.prototype.getTableNames = function(schemaNames){
    var self = this;
    return exports._getTableNames(self.knex,schemaNames);
};

exports.Query.prototype.getTableIds = function(schemaNames){
    var self = this;
    return exports._getTableIds(self.knex,schemaNames);
};

exports.Query.prototype.getColumnNames = function(schemaNames){
    var self = this;
    return exports._getColumnNames(self.knex,schemaNames);
};

exports.Query.prototype.getColumnTypes = function(schemaNames){
    var self = this;
    return exports._getColumnTypes(self.knex,schemaNames);
};