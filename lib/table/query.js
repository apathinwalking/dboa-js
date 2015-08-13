var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");
var column = require("../column/column.js");
var columns = require("../columns/columns.js");

exports.column = column;
exports.columns =  function(knex,fq_table_name){
  var newArgs = _.map(_.rest(_.rest(_.values(arguments))),function(a){
    a = [a];
    a.unshift(fq_table_name);
    return a;
  });
  newArgs.unshift(knex);
  var tmpFunc = _.spread(columns);
  return tmpFunc(newArgs);
}

exports.allInfo = function(knex,fq_table_name){
    return knex.select('*')
        .select('column_name',knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
        .from('information_schema.columns')
        .where(knex.raw('table_schema || \'.\' || table_name'),fq_table_name)
        .then(function(results){
            return results;
        });
};

exports.info = function(knex,fq_table_name,info_types){
    return knex.select(info_types)
        .select('column_name',knex.raw('table_schema || \'.\' || table_name as fq_table_name'))
        .from('information_schema.columns')
        .where(knex.raw('table_schema || \'.\' || table_name'),fq_table_name)
        .then(function(results){
            return _.map(results,function(r){
                return _.pick(r,info_types);
            });
        });
};

exports.columnNames = function(knex,fq_table_name){
    return exports.info(knex,fq_table_name,['column_name'])
        .then(function(results){
            return _.pluck(results,'column_name');
        });
};

exports.comment = function(knex,fq_table_name){
    return knex.select(knex.raw('\'' + fq_table_name + '\'::regclass')
        .wrap('obj_description(',');'))
        .then(function(results){
            return _.pluck(results,'obj_description')[0];
        },function(error){
            return null;
        });

};

exports.duplicates = function(knex,fq_table_name,match_column_names){
  var query_str = knex.select().from(fq_table_name).toString() + " as \"outer\" where (" + knex.count().from(fq_table_name).toString() + " as \"inner\" ";
  query_str += "where (" + helpers.parseTableColumn('inner',match_column_names[0]) + " = " + helpers.parseTableColumn('outer',match_column_names[0]) +
  " or ( " + helpers.parseTableColumn('inner',match_column_names[0]) + " is null and " + helpers.parseTableColumn('outer',match_column_names[0])  + " is null ) ) ";
  _.forEach(_.rest(match_column_names),function(m){
    query_str+= "and (" + helpers.parseTableColumn('inner',m) + " = " + helpers.parseTableColumn('outer',m) + " or ( " + helpers.parseTableColumn('inner',m) + " is null and " + helpers.parseTableColumn('outer',m)  + " is null ) ) ";
  });
  query_str += ") > 1";
  return knex.raw(query_str).then(function(results){
    return results.rows;
  });
};

exports.groupByDuplicates = function(knex,fq_table_name,match_column_names){
    return exports.duplicates(knex,fq_table_name,match_column_names)
        .then(function(results){
            var grouped =  _.groupBy(results,function(r){
                return _.values(_.pick(r,match_column_names));
            });
            //remove the keys
            return _.values(grouped);
        });
};

exports.deleteWhere = function(knex,fq_table_name,args){
    var query = knex(fq_table_name);
    query = query.where(args[0]);
    _.forEach(_.rest(args),function(a){
        query = query.orWhere(a);
    });
    return query.del()
        .then(function(results){
            return results;
        });
};

exports.deleteWhereNull = function(knex,fq_table_name,args){
  var query = knex(fq_table_name);
  query = query.whereNull(args[0]);
  _.forEach(_.rest(args),function(a){
      query = query.orWhereNull(a);
  });
  return query.del()
      .then(function(results){
          return results;
      });
};

//create a copy of a table
exports.copy = function(knex,fq_table_name,new_fq_table_name){
    return knex.raw('select * into ' + new_fq_table_name + ' from ' + fq_table_name)
        .then(function(results){
            //TODO: check if worked
            return true;
        });
};

exports.hasColumns = function(knex,fq_table_name,column_names){
  return exports.columnNames(knex,fq_table_name)
  .then(function(results){
    return _.reduce(column_names,function(obj,c){
      obj[c] = _.includes(results,c);
      return obj;
    },{})
  })
}
