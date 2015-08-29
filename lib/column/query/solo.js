var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../../helpers.js");
var tidyAddr = require('tidyaddr-js');
var rawQuery = require('../../raw.js');

exports.columnName = function(colObj) {
  return colObj.column_name;
};


exports.fqTableName = function(colObj) {
  return colObj.fq_table_name;
};

exports.tableName = function(colObj) {
  return exports.info(colObj, {
    info_types: 'table_name'
  }).then(function(results) {
    return _.pluck(results, 'table_name');
  });
};

exports.schemaName = function(colObj) {
  return exports.info(colObj, {
    info_types: 'table_schema'
  }).then(function(results) {
    return _.pluck(results, 'table_schema');
  });
};

exports.rowCount = function(colObj) {
  return colObj.knex.count()
    .from(colObj.fq_table_name)
    .then(function(results) {
      return parseInt(
        _.pluck(results, 'count')[0]
      );
    });
};

exports.notNullCount = function(colObj) {
  return colObj.knex.count(colObj.column_name)
    .from(colObj.fq_table_name)
    .then(function(results) {
      return parseInt(
        _.pluck(results, 'count')[0]
      );
    });
}

exports.nullCount = function(colObj) {
  return colObj.knex.count(colObj.column_name)
    .from(colObj.fq_table_name)
    .whereNull(colObj.column_name)
    .then(function(results) {
      return parseInt(
        _.pluck(results, 'count')[0]
      );
    });
};

exports.valueCount = function(colObj, args) {
  return colObj.knex.count(colObj.column_name)
    .from(colObj.fq_table_name)
    .where(colObj.column_name, args.value)
    .then(function(results) {
      return parseInt(
        _.pluck(results, 'count')[0]
      );
    });
};

exports.info = function(colObj, args) {
  return colObj.knex.select(args.info_types)
    .select('column_name', colObj.knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
    .from('information_schema.columns')
    .where('column_name', colObj.column_name)
    .where(colObj.knex.raw('table_schema || \'.\' || table_name'), colObj.fq_table_name)
    .then(function(results) {
      return _.map(results, function(r) {
        return _.pick(r, args.info_types);
      })[0];
    });
};

exports.allInfo = function(colObj) {
  return colObj.knex.select('*')
    .select('column_name', colObj.knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
    .from('information_schema.columns')
    .where('column_name', colObj.column_name)
    .where(colObj.knex.raw('table_schema || \'.\' || table_name'), colObj.fq_table_name)
    .then(function(results) {
      return results;
    });
};

exports.dataType = function(colObj) {
  return exports.info(colObj, {
      info_types: ['data_type']
    })
    .then(function(results) {
      return results.data_type;
    });
};

exports.udtName = function(colObj){
  return exports.info(colObj, {
      info_types: ['udt_name']
    })
    .then(function(results) {
      return results.data_type;
    });
}

exports.typeIn = function(colObj, args) {
  return exports.dataType(colObj)
    .then(function(results) {
      return _.includes(args.data_types, results);
    });
};

exports.min = function(colObj) {
  return colObj.knex.min(colObj.column_name)
    .from(colObj.fq_table_name)
    .then(function(results) {
      return _.pluck(results, 'min')[0];
    });
};

exports.max = function(colObj) {
  return colObj.knex.max(colObj.column_name)
    .from(colObj.fq_table_name)
    .then(function(results) {
      return _.pluck(results, 'max')[0];
    });
};

exports.ratio = function(colObj, args) {
  return Promise.all([
    exports.rowCount(colObj),
    exports.valueCount(colObj, {
      value: value
    })
  ]).spread(function(rowCount, valueCount) {
    return valueCount / rowCount;
  });
};

exports.sum = function(colObj) {
  return colObj.knex.sum(colObj.column_name)
    .from(colObj.fq_table_name)
    .then(function(results) {
      return _.pluck(results, 'sum')[0];
    }, function(error) {
      return null;
    });
};

exports.avg = function(colObj) {
  return colObj.knex.avg(colObj.column_name)
    .from(colObj.fq_table_name)
    .then(function(results) {
      return _.pluck(results, 'avg')[0];
    }, function(error) {
      return null;
    });
};

exports.distinct = function(colObj) {
  return colObj.knex.distinct(colObj.column_name)
    .from(colObj.fq_table_name)
    .pluck(colObj.column_name)
    .then(function(results) {
      return results;
    });
};

exports.distinctCount = function(colObj) {
  return colObj.knex.count()
    .from(function() {
      this.distinct(colObj.column_name)
        .from(colObj.fq_table_name)
        .as('tmp');
    })
    .then(function(results) {
      return parseInt(
        _.pluck(results, 'count')[0]
      );
    });
};

exports.distinctRatio = function(colObj) {
  return Promise.all([
    exports.distinctCount(colObj),
    exports.rowCount(colObj)
  ]).spread(function(distinctCount, rowCount) {
    return (distinctCount / rowCount);
  });
};

exports.comment = function(colObj) {
  return exports.info(colObj, {
      info_types: ['ordinal_position']
    })
    .then(function(results) {
      return colObj.knex.select(colObj.knex.raw('\'' + colObj.fq_table_name + '\'::regclass, ' + results.ordinal_position)
          .wrap('col_description(', ');'))
        .then(function(results) {
          return _.pluck(results, 'col_description')[0];
        }, function(error) {
          return null;
        });
    });
};

exports.exists = function(colObj) {
  return colObj.knex.select('column_name', colObj.knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
    .from('information_schema.columns')
    .where('column_name', colObj.column_name)
    .where(colObj.knex.raw('table_schema || \'.\' || table_name'), colObj.fq_table_name)
    .then(function(results) {
      if (results.length > 0) {
        return true;
      } else {
        return false;
      }
    });
};

exports.drop = function(colObj) {
  return colObj.knex.schema.table(colObj.fq_table_name, function(table) {
    table.dropColumn(colObj.column_name);
  });
};

exports.dropIfExists = function(colObj) {
  return exports.exists(colObj)
    .then(function(results) {
      if (results === true) {
        return exports.drop(colObj);
      } else {
        return false;
      }
    });
};

//args: data_type
exports.create = function(colObj,args){
  return colObj.knex.schema.table(colObj.fq_table_name, function(table){
    table.specificType(colObj.column_name,args.data_type);
  })
  .then(function(){
    return true;
  },function(error){
    console.log(error);
    process.exit(0);
    return false;
  })
}

exports.createIfNotExists = function(colObj,args){
  return colObj.knex.schema.table(colObj.fq_table_name, function(table){
    table.specificType(colObj.column_name,args.data_type);
  })
  .then(function(){
    return true;
  },function(error){
    return true;
  })
}

//args: column_name
exports.copy = function(colObj, args){
  //todo: fix copying outside of same table
  args = _.assign({},{fq_table_name:colObj.fq_table_name},args);
  newColObj = {
    knex:colObj.knex,
    fq_table_name:args.fq_table_name,
    column_name:args.column_name
  }
  return exports.dataType(colObj)
    .then(function(results){
      return exports.createIfNotExists(newColObj,{data_type:results});
    })
    .then(function(results){
      var query = rawQuery.copyColumn({fq_table_name:colObj.fq_table_name,copy_from_column_name:colObj.column_name,copy_to_column_name:args.column_name});
      return colObj.knex.raw(query);
    })
    .then(function(results){
      return true;
    },function(error){
      console.log(error);
    });
};

//args: [conditions] ex: [{where:['p','=','10'],update:1},{where:['p','<','5'],update:5}]
exports.updateWhere = function(colObj, args){
  var promises = _.map(args.conditions, function(c){
    var subQuery = colObj.knex(colObj.fq_table_name).update(colObj.column_name,c.update);
    if(Array.isArray(c.where) && c.where.length === 2){
      subQuery = subQuery.where(c.where[0],c.where[1]);
    }
    else if(Array.isArray(c.where) && c.where.length === 3){
      subQuery = subQuery.where(c.where[0],c.where[1],c.where[2]);
    }
    else{
      subQuery = subQuery.where(c.where);
    }
    return subQuery.then(function(results){
      return true;
    },function(error){
      console.log(error);
      return false;
    })
  });
  return Promise.all(promises);
}
//args: [column_name]
exports.tidyAddrLot = function(colObj, args){
  args = _.assign({},{fq_table_name:colObj.fq_table_name},args);
  newColObj = {
    knex:colObj.knex,
    fq_table_name:args.fq_table_name,
    column_name:args.column_name
  };
  return exports.create(newColObj,{data_type:'text'})
    .then(function(){
      var query = rawQuery.updateFromRegex({
        'fq_table_name':colObj.fq_table_name,
        'input_column_name':colObj.column_name,
        'output_column_name':args.column_name,
        'regex':'^0*',
        'replace':''
      });
    console.log(query);
    return colObj.knex.raw(query);
    })
    .then(function(){
      return true;
    })
}

//args: [column_name, verbose, chunksize, tmp_table_name,tmp_column_name];
exports.tidyAddr = function(colObj, args) {
  args = _.assign({}, {
    column_name: 'tidy_address',
    chunksize: 1000,
    tmp_table_name: "_tmp_tbl",
    tmp_column_name: colObj.column_name + "_tmp_idx"
  }, args);
  var data = {};
  return colObj.knex.schema.table(colObj.fq_table_name, function(table) {
      table.specificType(args.tmp_column_name, "bigserial");
      table.text(args.column_name);
      //TODO: fix the fact that this will delete the args.column_name if it already exists;
    }).then(function() {
      var query = "CREATE TABLE " + args.tmp_table_name + "(" + args.column_name + " text, " + args.tmp_column_name + " bigint )";
      return colObj.knex.raw(query);
    }).then(function() {
      return exports.rowCount(colObj);
    }).then(function(results) {
      data.rowCount = results;
      var range = _.range(0, data.rowCount);
      var chunks = _.chunk(range, args.chunksize);
      return Promise.map(chunks, function(chunk) {
        var min = _.min(chunk);
        var max = _.max(chunk);
        return colObj.knex.select(colObj.column_name, args.tmp_column_name).from(colObj.fq_table_name).whereBetween(args.tmp_column_name, [min, max])
          .then(function(results) {
            var tidyAddrs = _.map(results, function(r) {
              var obj = {};
              addrLine = r[colObj.column_name];
              var tidyed = tidyAddr.cleanLine(addrLine);
              obj[args.tmp_column_name] = r[args.tmp_column_name];
              obj[args.column_name] = tidyed.tidyaddress;
              return obj;
            });
            if (args.verbose) {
              console.log("cleaned " + max + "/" + data.rowCount + " lines from " + colObj.fq_table_name);
              //console.log(process.memoryUsage());
            }
            return colObj.knex(args.tmp_table_name).insert(tidyAddrs);
          });
      });
    })
    .then(function(results) {
      var queryStr = ''
    })
    .then(function(results) {
      if (args.verbose) {
        console.log("inserted into tmp table");
      }
      var query = rawQuery.updateSharedColumnOnSharedIndex({
        'update_into_fq_table_name': colObj.fq_table_name,
        'update_from_fq_table_name': args.tmp_table_name,
        'shared_column_name': args.column_name,
        'shared_index': args.tmp_column_name
      });
      console.log(query);
      return colObj.knex.raw(query);
    })
    .then(function(results) {
      return onFin().then(function() {
        return true;
      });
    }, function(error) {
      console.log(error);
      return onErr().then(function() {
        return false;
      });
    });


  function onFin() {
    return colObj.knex.schema.dropTable(args.tmp_table_name)
      .then(function(results) {
        return colObj.knex.schema.table(colObj.fq_table_name, function(table) {
          table.dropColumn(args.tmp_column_name);
        });
      });
  }

  function onErr() {
    return colObj.knex.schema.dropTable(args.tmp_table_name)
      .then(function(results) {
        return colObj.knex.schema.table(colObj.fq_table_name, function(table) {
          table.dropColumn(args.tmp_column_name);
          table.dropColumn(args.column_name);
        });
      }, function(error) {
        return colObj.knex.schema.table(colObj.fq_table_name, function(table) {
          table.dropColumn(args.tmp_column_name);
          table.dropColumn(args.column_name);
        }).then(function(){
          process.exit(0);
        });
      });
  }
};
