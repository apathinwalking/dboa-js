var Q = require('q');
var _ = require('lodash');
var help = require('./../helpers.js');

//TODO: data type frequency

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
    exports.getColumnTypes(knex,tableIds)
        .then(function(rows){
            deferred.resolve(help.mergeOn(rows,'data_type'));
        });
    return deferred.promise;
};

exports.getDistinctColumnTypesCount = function(knex,tableIds){
    "use strict";
    var deferred = Q.defer();
    exports.getDistinctColumnTypes(knex,tableIds)
        .then(function(rows){
            rows = _.map(rows,function(r){
                r.count = r.column_name.length;
                return r;
            });
            deferred.resolve(rows);
        });
    return deferred.promise;
};

exports.getDistinctColumnValues = function(knex,tableIds){
    "use strict";
    var deferred = Q.defer();
    exports.getColumnNames(knex,tableIds)
        .then(function(rows){
            var funcs = _.map(rows,function(r){
                return(require('../column/query.js').getDistinctValues(knex, r.table_id,[r.column_name]));
            });
            return Q.all(funcs);
        })
        .then(function(rows){
            deferred.resolve(_.flatten(rows));
        });
    return deferred.promise;
};

exports.Query = help.mapKnex(exports);
