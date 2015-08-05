var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");
var query = require("./query.js");

function Column(knex,column_name){
  var _this = this;
  _.forIn(query,function(value,key){
      _this[key] = helpers.recursiveWrap([knex,column_name],value);
  });
};

module.exports = function(knex,column_name){
  return new Column(knex,column_name);
}
