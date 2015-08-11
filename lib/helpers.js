var Promise = require('bluebird');
var _ = require('lodash');

exports.numericDataTypes = [
    "smallint",
    "int",
    "bigint",
    "real",
    "double",
    "precision",
    "numeric",
    "interval"
];

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
    },[]);
};

exports.wrapAll = function(value,obj){
    return _.mapValues(obj,function(m){
        return _.wrap(value,m);
    });
};

exports.recursiveWrap = function(values,func){
    return _.reduce(_.rest(values),function(results,val){
      results = _.wrap(val,results);
      return results;
    },_.wrap(values[0],func));
};

exports.recursiveWrapAll = function(values,obj){
    return _.mapValues(obj,function(m){
      return exports.recursiveWrap(values,m);
    });
};

exports.recursiveWrapSpread = function(values_arr,func){
    return function(){
      var argsArr = _.values(arguments);
      if(argsArr.length == 0){
        return _.spread(func)(values_arr);
      }
      var combinedValues = _.map(_.range(values_arr.length),function(i){
        var flat =  _.flatten([values_arr[i],argsArr[i]]);
        return flat;
      });
      return _.spread(func)(combinedValues);
    };
};

exports.multiWrapPromise = function(func){
  var newFunc = function(){
    var argsArr = _.values(arguments);
    var promises = _.map(argsArr,function(a){
      return _.spread(func)(a);
    });
    return Promise.all(promises).then(function(results){
      return results;
    });
  };
  return newFunc;
};



exports.identityColumn = function(knex,value,column_name){
    var string = "\'" + value + "\'" + " " + column_name;
    return knex.raw(string);
};

exports.nullColumn = function(knex,column_name){
    return knex.raw("null " + column_name);
};

exports.parseTableColumn = function(table_name,column_name){
  return '\"' + table_name + '\"' + '.' + '\"' + column_name + '\"';
};
