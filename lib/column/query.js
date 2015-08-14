var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");

exports.column = {};

exports.column.columnName = function(colObj) {
  return colObj.column_name;
};

exports.column.fqTableName = function(colObj) {
  return colObj.fq_table_name;
};

exports.column.tableName = function(colObj) {
  return exports.column.info(colObj, {
    info_types: 'table_name'
  }).then(function(results) {
    return _.pluck(results, 'table_name');
  });
};

exports.column.schemaName = function(colObj) {
  return exports.column.info(colObj, {
    info_types: 'table_schema'
  }).then(function(results) {
    return _.pluck(results, 'table_schema');
  });
};

exports.column.rowCount = function(colObj) {
  return colObj.knex.count(colObj.column_name)
    .from(colObj.fq_table_name)
    .then(function(results) {
      return parseInt(
        _.pluck(results, 'count')[0]
      );
    });
};

exports.column.nullCount = function(colObj) {
  return colObj.knex.count(colObj.column_name)
    .from(colObj.fq_table_name)
    .whereNull(colObj.column_name)
    .then(function(results) {
      return parseInt(
        _.pluck(results, 'count')[0]
      );
    });
};

exports.column.valueCount = function(colObj, args) {
  return colObj.knex.count(colObj.column_name)
    .from(colObj.fq_table_name)
    .where(colObj.column_name, args.value)
    .then(function(results) {
      return parseInt(
        _.pluck(results, 'count')[0]
      );
    });
};

exports.column.info = function(colObj, args) {
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

exports.column.allInfo = function(colObj) {
  return colObj.knex.select('*')
    .select('column_name', colObj.knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
    .from('information_schema.columns')
    .where('column_name', colObj.column_name)
    .where(colObj.knex.raw('table_schema || \'.\' || table_name'), colObj.fq_table_name)
    .then(function(results) {
      return results;
    });
};

exports.column.dataType = function(colObj) {
  return exports.column.info(colObj, {
      info_types: ['data_type']
    })
    .then(function(results) {
      return _.pluck(results, 'data_type')[0];
    });
};

exports.column.typeIn = function(colObj, args) {
  return exports.column.dataType(colObj)
    .then(function(results) {
      return _.includes(args.data_types, results);
    });
};

exports.column.min = function(colObj) {
  return colObj.knex.min(colObj.column_name)
    .from(colObj.fq_table_name)
    .then(function(results) {
      return _.pluck(results, 'min')[0];
    });
};

exports.column.max = function(colObj) {
  return colObj.knex.max(colObj.column_name)
    .from(colObj.fq_table_name)
    .then(function(results) {
      return _.pluck(results, 'max')[0];
    });
};

exports.column.ratio = function(colObj, args) {
  return Promise.all([
    exports.column.rowCount(colObj),
    exports.column.valueCount(colObj, {
      value: value
    })
  ]).spread(function(rowCount, valueCount) {
    return valueCount / rowCount;
  });
};

exports.column.sum = function(colObj) {
  return colObj.knex.sum(colObj.column_name)
    .from(colObj.fq_table_name)
    .then(function(results) {
      return _.pluck(results, 'sum')[0];
    }, function(error) {
      return null;
    });
};

exports.column.avg = function(colObj) {
  return colObj.knex.avg(colObj.column_name)
    .from(colObj.fq_table_name)
    .then(function(results) {
      return _.pluck(results, 'avg')[0];
    }, function(error) {
      return null;
    });
};

exports.column.distinct = function(colObj) {
  return colObj.knex.distinct(colObj.column_name)
    .from(colObj.fq_table_name)
    .pluck(colObj.column_name)
    .then(function(results) {
      return results;
    });
};

exports.column.distinctCount = function(colObj) {
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

exports.column.distinctRatio = function(colObj) {
  return Promise.all([
    exports.column.distinctCount(colObj),
    exports.column.rowCount(colObj)
  ]).spread(function(distinctCount, rowCount) {
    return (distinctCount / rowCount);
  });
};

exports.column.comment = function(colObj) {
  return exports.column.info(colObj, {
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

exports.columns = {};
