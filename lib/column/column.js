var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");
var query = require("./query.js");

function Column(knex,fq_table_name,column_name){
  var _this = this;
  _.forIn(query,function(value,key){
      _this[key] = helpers.recursiveWrap([knex,fq_table_name,column_name],value);
  });
};

module.exports = function(knex,fq_table_name,column_name){
  return new Column(knex,fq_table_name,column_name);
}
