var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");
var query = require("./query.js");

function Columns(knex, table_column_name_pairs){
  var _this = this;
  _.forIn(query,function(value,key){
      var q =  _.wrap(knex,value);
      _this[key] = helpers.recursiveWrapSpread(table_column_name_pairs,q);
  });
}

module.exports = function(knex /** table_column_name_pairs**/){
    var argsArr = _.values(arguments);
  return new Columns(knex,_.rest(argsArr));
};
