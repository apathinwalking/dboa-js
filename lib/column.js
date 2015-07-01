var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("./helpers.js");
var query = require("./query.js");

var Column = function(knex,args){
    var _this = this;

    _this.knex = knex;

    _.forIn(args,function(value,key){
        _this[key] = value;
    });

    //check for table_name table_schema pair or fq_table_name
    _.bind(helpers.resolveFqTableName,_this)();
    if(!_this.column_name){
        throw new Error("No column_name provided");
    }
};

Column.prototype.rowCount = function(){
    var _this = this;
    if(_this.row_count){
        return Promise.resolve(_this);
    }
    else{
        return Promise.resolve(query.column.rowCount(_this.knex,_this.fq_table_name,_this.column_name))
            .then(function(results){
                _this.row_count = results;
                return _this;
            });
    }
};

Column.prototype.nullCount = function(){
    var _this = this;
    if(_this.null_count){
        return Promise.resolve(_this);
    }
    else{
        return Promise.resolve(query.column.nullCount(_this.knex,_this.fq_table_name,_this.column_name))
            .then(function(results){
                _this.null_count = results;
                return _this;
            });
    }
};

Column.prototype.dataType = function(){
    var _this = this;
    if(_this.data_type){
        return Promise.resolve(_this);
    }
    else {
        return Promise.resolve(query.column.nullCount(_this.knex, _this.fq_table_name, _this.column_name))
            .then(function (results) {
                _this.data_type = results;
                return _this;
            });
    }
};

Column.prototype.compare = function(column,callback){
    return callback(this,column);
};

exports.create = function(knex,args){
    return new Column(knex,args);
};

