var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("./helpers.js");
var query = require("./query.js");
var column = require("./column.js");
var table = require('./table.js');

function TableGroup(knex,tableArgs,args){
    var _this = this;
    _this.knex = knex;
    _this.tables = _.map(tableArgs, function(args){
        return table.create(_this.knex,args);
    });
}

TableGroup.prototype.initialize = function(){
    var _this = this;
    var promises = _.map(_this.tables,function(t){
        return t.initialize();
    });
    return Promise.all(promises)
        .then(function(results){
            _this.tables = results;
            return _this;
        });
};


exports.create = function(knex,tableArgs,args){
    return new TableGroup(knex,tableArgs,args);
};

exports.initialize = function(knex,tableArgs,args){
    var tableGroup = new TableGroup(knex,tableArgs,args);
    return tableGroup.initialize();
};