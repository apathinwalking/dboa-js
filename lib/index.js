var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require('./helpers.js');
var column = require('./column/column.js');

function Dboa(dbConfig){
    var _this = this;
    _this.knex = require('knex')(dbConfig);

    _this.column = column;
}


module.exports = function(dbConfig){
    return new Dboa(dbConfig);
};
