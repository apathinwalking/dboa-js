var Q = require('q');
var _ = require('lodash');
var help = require('./../helpers.js');

exports._getType = function(knex,tableId,columnNames){
    "use strict";
    var deferred = Q.defer();
    knex.select('data_type','column_name',knex.raw('table_schema || \'.\' || table_name as table_id'))
        .from('information_schema.columns')
        .where(knex.raw('table_schema || \'.\' || table_name'),tableId)
        .whereIn('column_name',columnNames)
        .then(function(rows){
            deferred.resolve(rows);
        });
    return deferred.promise;
};

exports._getDistinctValues = function(knex,tableId,columnNames){
    "use strict";
    var deferred = Q.defer();
    var queries = _.map(columnNames, function(col){
        var deferred = Q.defer();
        knex.distinct(col).from(tableId)
            .then(function(rows){
                deferred.resolve(_.map(rows,function(row){
                    return({
                        'table_id':tableId,
                        'val':row[col],
                        'column_name':col
                    });
                }));
            });
        return deferred.promise;
    });
    Q.all(queries)
        .then(function(rows){
            deferred.resolve(_.flatten(rows));
        });
    return deferred.promise;
};

exports.Query = function(knex){
    "use strict";
    this.knex = knex;
};

exports.Query.prototype.getType = function(tableId, columnNames){
    var self = this;
    return exports._getType(self.knex,tableId,columnNames);
};

exports.Query.prototype.getDistinctValues = function(tableId, columnNames){
    var self = this;
    return exports._getDistinctValues(self.knex,tableId,columnNames);
}