var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require('./helpers.js');
var column = require('./column/column.js');
//var columns = require('./columns/columns.js');
var table = require('./table/table.js');
//var tables = require('./tables/tables.js');

function Dboa(dbConfig) {
  var _this = this;
  _this.knex = require('knex')(dbConfig);
  _this.table = helpers.wrapAssign({
    knex: _this.knex
  }, table);
  _this.column = helpers.wrapAssign({
    knex: _this.knex
  }, column);


}


module.exports = function(dbConfig) {
  return new Dboa(dbConfig);
};
