var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");
var tableQuery = require('../table/query.js');

exports.imported = {};

_.forIn(tableQuery, function(value,key){
  var tmpFunc = helpers.multiWrapPromise(value);
  exports.imported[key] = function(knex){
    var newArgs = _.map(_.rest(_.values(arguments)),function(a){
      a.unshift(knex);
      return a;
    });
    var tmpFunc2 = _.spread(tmpFunc);
    return tmpFunc2(newArgs).then(function(results){
      return results;
    });
  }
});

exports.original = {};

exports.original.eachHasColumns = function(knex,fq_table_names,column_names){
  var promises = _.map(fq_table_names,function(t){
    return tableQuery.hasColumns(knex,t,column_names);
  });
  return Promise.all(promises);
};

exports.original.columnNamesIntersection = function(knex,fq_table_names){
  return exports.imported.columnNames(knex,fq_table_names)
  .then(function(results){
    return _.intersection(results)[0];
  });
};

exports.original.columnNamesUnion = function(knex,fq_table_names){
  return exports.imported.columnNames(knex,fq_table_names)
  .then(function(results){
    return _.union(results)[0];
  })
};
