var Q = require('q');
var _ = require('lodash');

exports._getSchemaNames = function(knex){
    "use strict";
    var deferred = Q.defer();
    knex.distinct('table_schema')
        .from('information_schema.tables')
        .then(function(rows){
            deferred.resolve(rows);
        });
    return deferred.promise;
};

exports._getTableNames = function(knex){
    "use strict";
    var deferred = Q.defer();
    exports._getSchemaNames(knex)
        .then(function(rows){
            var funcs = _.map(rows,function(r){
                return(require('../schema/query.js')._getTableNames(knex,[r.table_schema]));
            });
            return(Q.all(funcs));
        })
        .then(function(rows){
            deferred.resolve(_.flatten(rows));
        });
    return deferred.promise;
};

exports._getTableIds = function(knex){
    "use strict";
    var deferred = Q.defer();
    exports._getSchemaNames(knex)
        .then(function(rows){
            var funcs = _.map(rows,function(r){
                return(require('../schema/query.js')._getTableIds(knex,[r.table_schema]));
            });
            return(Q.all(funcs));
        })
        .then(function(rows){
            deferred.resolve(_.flatten(rows));
        });
    return deferred.promise;
};

exports.Query = function(knex){
    this.knex = knex;
};

exports.Query.prototype.getSchemaNames = function(){
    var self = this;
    return exports._getSchemaNames(self.knex);
};

exports.Query.prototype.getTableNames = function(){
    var self = this;
    return exports._getTableNames(self.knex);
};

exports.Query.prototype.getTableIds = function(){
    var self = this;
    return exports._getTableIds(self.knex);
};
