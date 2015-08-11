var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");
var query = require("./query.js");

function Table(knex,fq_table_name){
  var _this = this;
  _.forIn(query,function(value,key){
    _this[key] = helpers.recursiveWrap([knex,fq_table_name],value);
  });
};

module.exports = function(knex,fq_table_name){
  return new Table(knex,fq_table_name);
}
