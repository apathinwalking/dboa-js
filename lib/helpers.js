var Promise = require('bluebird');
var _ = require('lodash');

exports.resolveFqTableName = function(){
    var _this = this;
    var getTableSchemaAndTableName = function(){
        var split = _this.fq_table_name.split(".");
        if(split.length !== 2){
            throw new Error("Not a valid fq_table_name. Should be table_schema.table_name");
        }
        var tmp =  {table_schema:split[0],table_name:split[1]};
        if(_this.table_schema){
            if(tmp.table_schema !== _this.table_schema){
                throw new Error("fq_table_name does not match table_schema");
            }
        }
        if(_this.table_name){
            if(tmp.table_name !== _this.table_name){
                throw new Error("fq_table_name does not match table_name");
            }
        }
        _this.table_name = tmp.table_name;
        _this.table_schema = tmp.table_schema;
    };

    var getFqTableName = function(){
        _this.fq_table_name = _this.table_schema + "." + _this.table_name;
    };

    if(!_this.table_schema && !_this.table_name && !_this.fq_table_name){
        throw new Error("Not enough information provided. Need a table_name and table_schema pair or fq_table_name");
    }
    else if(!_this.table_schema && !_this.table_name && _this.fq_table_name){
        getTableSchemaAndTableName();
    }
    else if(!_this.table_schema && _this.table_name && !_this.fq_table_name){
        throw new Error("Not enough information provided. Need a table_name and table_schema pair or fq_table_name");
    }
    else if(!_this.table_schema && _this.table_name && _this.fq_table_name){
        getTableSchemaAndTableName();
    }
    else if(_this.table_schema && !_this.table_name && !_this.fq_table_name){
        throw new Error("Not enough information provided. Need a table_name and table_schema pair or fq_table_name");
    }
    else if(_this.table_schema && !_this.table_name && _this.fq_table_name){
        getTableSchemaAndTableName();
    }
    else if(_this.table_schema && _this.table_name && !_this.fq_table_name){
        getFqTableName();
    }
    else if(_this.table_schema && _this.table_name && _this.fq_table_name){
        getTableSchemaAndTableName();
    }
};

exports.pairColumns = function(table1,table2){
    return _.reduce(table1.columns,function(results,column1){
        _.forEach(table2.columns,function(column2){
            results.push([column1,column2]);
        });
        return results;
    },[])
};