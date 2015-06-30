var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("./helpers.js");
var queries = require("./queries.js");
var column = require("./column.js");

var Table = function(knex,args){
    var _this = this;

    _this.knex = knex;
    _.forIn(args,function(value,key){
        _this[key] = value;
    });
    //check for table_name table_schema pair or fq_table_name
    _.bind(helpers.resolveFqTableName,_this)();
};

Table.prototype.initialize = function(){
    var _this = this;
    _this.columns = [];
    return Promise.resolve(queries.table.allInfo(_this.knex,_this.fq_table_name))
        .then(function(results){
            _.forEach(results,function(r){
                _this.columns.push(column.create(_this.knex,r));
            });
            return _this;
        });
};

Table.prototype.compare = function(table,callback){
    return callback(_this,table);
};

Table.prototype.compareColumns = function(table,callback){
    var _this = this;
    var paired = helpers.pairColumns(_this,table);
    var promises = [];
    _.forEach(paired, function(p){
        var promise = Promise.resolve(callback(p[0], p[1]))
            .then(function(result){
                return {columns:p,value:result};
            });
        promises.push(promise);
    });
    return Promise.all(promises);
};

Table.prototype.filterColumns = function(table,callback){
    return this.compareColumns(table,callback)
        .then(function(results){
            return _.filter(results,function(r){
                return r.value === true;
            });
        });
};

Table.prototype.partitionColumns = function(table,callback){
    return this.compareColumns(table,callback)
        .then(function(results){
            return _.partition(results,function(r){
                return r.value === true;
            });
        });
};

Table.prototype.groupByColumns = function(table,callback){
    return this.compareColumns(table,callback)
        .then(function(results){
            return _.groupBy(results,'value');
        });
};

exports.create = function(knex,args){
    var table = new Table(knex,args);
    return table;
};

exports.initialize = function(knex,args){
    var table = new Table(knex,args);
    return table.initialize();
};