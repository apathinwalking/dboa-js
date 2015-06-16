var Q = require('q');
var _ = require('lodash');
var help = require('./../helpers.js');

exports._getColumnNames = function(knex,tableIds){
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

exports._getColumnTypes = function(knex,tableIds){
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

exports._getDistinctColumnTypes = function(knex,tableIds){
    "use strict";
    var deferred = Q.defer();
    exports._getColumnTypes(knex,tableIds)
        .then(function(rows){
            deferred.resolve(help.mergeOn(rows,'data_type'));
        });
    return deferred.promise;
};

exports._getDistinctColumnValues = function(knex,tableIds){
    "use strict";
    var deferred = Q.defer();
    exports.getColumnNames(knex,tableIds)
        .then(function(rows){
            var funcs = _.map(rows,function(r){
                return(require('../column/query.js')._getDistinctValues(knex, r.table_id,[r.column_name]));
            });
            return Q.all(funcs);
        })
        .then(function(rows){
            deferred.resolve(_.flatten(rows));
        });
    return deferred.promise;
};

exports.Query = function(knex){
    this.knex = knex;
};

exports.Query.prototype.getColumnNames = function(tableIds){
    var self = this;
    return exports._getColumnNames(self.knex,tableIds);
};

exports.Query.prototype.getColumnTypes = function(tableIds){
    var self = this;
    return exports._getColumnTypes(self.knex,tableIds);
};

exports.Query.prototype.getDistinctColumnTypes = function(tableIds){
    var self = this;
    return exports._getDistinctColumnTypes(self.knex,tableIds);
};

exports.Query.prototype.getDistinctColumnValues = function(tableIds){
    var self = this;
    return exports._getDistinctColumnValues(self.knex,tableIds);
};