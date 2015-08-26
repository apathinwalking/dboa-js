var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../../helpers.js");
var tidyAddr = require('tidyaddr-js');

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
  return colObj.knex.count(colObj.column_name)
    .from(colObj.fq_table_name)
    .then(function(results) {
      return parseInt(
        _.pluck(results, 'count')[0]
      );
    });
};

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

exports.exists = function(colObj){
  return colObj.knex.select('column_name',colObj.knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
    .from('information_schema.columns')
    .where('column_name',colObj.column_name)
    .where(colObj.knex.raw('table_schema || \'.\' || table_name'), colObj.fq_table_name)
    .then(function(results){
      if(results.length > 0){
        return true;
      }
      else{
        return false;
      }
    });
}

exports.drop = function(colObj){
  return colObj.knex.schema.table(colObj.fq_table_name,function(table){
    table.dropColumn(colObj.column_name);
  });
};

exports.dropIfExists = function(colObj){
  return exports.exists(colObj)
    .then(function(results){
      if(results === true){
        return exports.drop(colObj);
      }
      else{
        return false;
      }
    });
};

//args: [column_name, verbose];
exports.tidyAddr = function(colObj,args){
  args = _.assign({},{column_name:'tidy_address'},args);
  var data = {};
  return colObj.knex.schema.table(colObj.fq_table_name, function(table){
    table.specificType("_tmp_idx","bigserial");
    table.text(args.column_name);
  }).then(function(){
    return exports.rowCount(colObj);
  }).then(function(results){
    data.rowCount = results;
    return Promise.map(_.range(0,data.rowCount),function(i){
      return colObj.knex.select(colObj.column_name).from(colObj.fq_table_name).where('_tmp_idx',i).limit(1)
      .then(function(results){
        var addrLine;
        if(results.length === 0){
          addrLine = null;
        }
        else{
          addrLine = results[0][colObj.column_name];
        }
        var tidyed = tidyAddr.cleanLine(addrLine);
        var updateObj = {};
        updateObj[args.column_name] = tidyed.tidyaddress;
        return colObj.knex(colObj.fq_table_name).where('_tmp_idx',i).update(updateObj)
          .then(function(){
            if(args.verbose){
              console.log("cleaned " + i + "/" + data.rwoCount + " lines from " + colObj.fq_table_name);
            }
          });
      });
    }).then(function(results){
      return results;
    });
  }).then(function(results){
    return colObj.knex.schema.table(colObj.fq_table_name, function(table){
      table.dropColumn('_tmp_idx');
    });
  }).then(function(results){
    return true;
  });
};
