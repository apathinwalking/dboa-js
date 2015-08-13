var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");
var columnQuery = require('../column/query.js');

exports.imported =  {};

_.forIn(columnQuery, function(value,key){
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
