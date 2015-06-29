"use strict";
var Promise = require('bluebird');
var _ = require('lodash');
var table = require('./table.js');
var column = require('./column.js');

function Dboa(dbConfig){
    var self = this;
    this.knex = require('knex')(dbConfig);
}

Dboa.prototype.createTables = function(argsArray){
    var _this = this;
    var promises = [];
    _.forEach(argsArray,function(args){
        promises.push(table.createTable(_this.knex,args));
    });
    return Promise.all(promises);
};

Dboa.prototype.createTable = function(args){
    return table.CreateTable(this.knex,args);
};

Dboa.prototype.createColumn = function(args){
    return column.createColumn(this.knex,args);
};


module.exports = function(dbConfig){
    return new Dboa(dbConfig);
};