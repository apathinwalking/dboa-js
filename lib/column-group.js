var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("./helpers.js");
var query = require("./query.js");
var column = require("./column.js");

function ColumnGroup(knex,columnArgs,args){
    var _this = this;
    _this.knex = knex;
    _this.columns = _.map(columnArgs, function(args){
        return column.create(_this.knex,args);
    });
}

ColumnGroup.prototype.apply = function(functionName,args){
    var _this = this;
    var promises = _.map(_this.columns,function(c){
        return c[functionName](args);
    });
    return Promise.all(promises)
        .then(function(results){
            _this.columns = results;
            return _this;
        });
};

ColumnGroup.prototype.initialize = function(){};


exports.create = function(knex,tableArgs,args){
    return new ColumnGroup(knex,tableArgs,args);
};

exports.initialize = function(knex,tableArgs,args){};