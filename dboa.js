var Column = require('./lib/column/index.js');
var Table = require('./lib/table/index.js');
var Schema = require('./lib/schema/index.js');
var Db = require('./lib/db/index.js');

var Dboa = function(connection_options){
    "use strict";
    var knex = require('knex')(connection_options);
    this.column = new Column(knex);
    this.table = new Table(knex);
    this.schema = new Schema(knex);
    this.db = new Db(knex);
};

module.exports = Dboa;
