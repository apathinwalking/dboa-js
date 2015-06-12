exports.column = require('./lib/column/column.js');
exports.table = require('./lib/table/table.js');

/*var Q = require('q');
var _columnQuery = require('./lib/column/query.js');
var _tableQuery = require('./lib/table/query.js');
var _tableCompare = require('./lib/table/compare.js');*/

/*var Dboa = function(connection_options){
    "use strict";
    if(connection_options.client !== 'pg' || connection_options.client === undefined){
        throw(new Error("Only pg is accepted as a client (for now)"));
    }
    this.knex = require('knex')(connection_options);
};*/


//module.exports = Dboa;