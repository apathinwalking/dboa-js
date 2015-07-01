"use strict";
var Promise = require('bluebird');
var _ = require('lodash');
var table = require('./table.js');
var tableGroup = require('./table-group.js');
var column = require('./column.js');
var columnGroup = require('./column-group.js');
var query = require('./query.js');
var helpers = require('./helpers.js');

function Dboa(dbConfig){
    var _this = this;
    _this.knex = require('knex')(dbConfig);

    _this.column = helpers.wrapAll(_this.knex,column);
    _this.column.query = helpers.wrapAll(_this.knex,query.column);

    _this.columnGroup = helpers.wrapAll(_this.knex,columnGroup);

    _this.table = helpers.wrapAll(_this.knex,table);
    _this.table.query = helpers.wrapAll(_this.knex,query.table);

    _this.tableGroup = helpers.wrapAll(_this.knex,tableGroup);

    _this.utils = helpers.wrapAll(_this.knex,query.utils);
}


module.exports = function(dbConfig){
    return new Dboa(dbConfig);
};