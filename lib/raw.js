var _ = require('lodash');
var helpers = require("./helpers.js");

//primary_fq_table_name, foreign_fq_table_name, primary_column_name, foreign_column_name, primary_data_column_name, foreign_data_column_name
exports.linkColumns = function(args){
  return _.template(
    'UPDATE <%= qu(primary_fq_table_name) %> AS "t1" SET <%= qu(primary_column_name) %> = "t2".<%= foreign_column_name%> FROM <%= qu(foreign_fq_table_name) %> AS "t2" WHERE "t1".<%= qu(primary_data_column_name) %> = "t2".<%= qu(foreign_data_column_name) %>;'
    ,{imports:{qu:helpers.quoteDotSep,sq:helpers.singleQuote}})(args);
}

//fq_table_name, input_column_name, output_column_name,regex, replace
exports.updateFromRegex = function(args){
  return _.template(
    'UPDATE <%= qu(fq_table_name) %> SET <%= qu(output_column_name) %> = regexp_replace( <%= qu(input_column_name) %>, <%= sq(regex) %> , <%= sq(replace) %> );'
    ,{imports:{qu:helpers.quoteDotSep,sq:helpers.singleQuote}})(args);
}

//fq_table_name ,copy_from_column_name, copy_to_column_name
exports.copyColumn = function(args){
  return _.template(
    'UPDATE <%= qu(fq_table_name) %> SET <%= qu(copy_to_column_name) %> = <%= qu(copy_from_column_name) %>'
    ,{imports:{qu:helpers.quoteDotSep}})(args);
}

//update_into_fq_table_name, update_from_fq_table_name,shared_column_name, shared_index
exports.updateSharedColumnOnSharedIndex = function(args){
  return _.template(
    'UPDATE <%= qu(update_into_fq_table_name) %> AS "t1" SET <%= qu(shared_column_name) %> = "t2".<%= qu(shared_column_name) %> FROM <%= qu(update_from_fq_table_name) %> AS "t2" WHERE "t1".<%= qu(shared_index) %> = "t2".<%= qu(shared_index) %>;'
  ,{imports:{qu:helpers.quoteDotSep}})(args);
}

//range, n
exports.selectRandomInts = function(args){
    return _.template(
        'SELECT (TRUNC(RANDOM() * <%= range %> + 1)) FROM GENERATE_SERIES(0,<%= n %>);'
    )(args);
};

//fq_table_name, column_names
exports.colsToJson = function(args){
    return _.template(
        'json_build_object( <%= _.flatten(_.map(column_names, function(c){ return [sq(c),qu(fq_table_name + "." + c)]})).join(",") %> )::json'
    ,{imports:{qu:helpers.quoteDotSep,sq:helpers.singleQuote}})(args);
}

//fq_table_name, column_names
exports.colsToArray = function(args){
  return _.template(
      'array[ <%= _.map(column_names, function(c){ return "" + qu(fq_table_name + "." + c) + "::text"}).join(",") %> ]'
  ,{imports:{qu:helpers.quoteDotSep,sq:helpers.singleQuote}})(args);
}

//fq_table_name, column_names
exports.colsToJsonb = function(args){
    return _.template(
        'json_build_object( <%= _.flatten(_.map(column_names, function(c){ return [sq(c),qu(fq_table_name + "." + c)]})).join(",") %> )::jsonb'
    ,{imports:{qu:helpers.quoteDotSep,sq:helpers.singleQuote}})(args);
}

//args: fq_table_name, partition_column_name, partitioning_column_name
exports.updatePartitionColumn = function(args){
    return  _.template(
        'UPDATE <%= qu(fq_table_name) %> AS "t1" \n' +
        'SET <%= qu(partition_column_name) %> = "t2".<%= qu(partition_column_name) %> \n' +
        'FROM(\n' +
        'SELECT <%= qu(partitioning_column_name) %>, DENSE_RANK() OVER (ORDER BY <%= qu(partitioning_column_name) %>) <%= qu(partition_column_name) %> \n' +
        'FROM <%= qu(fq_table_name) %> \n' +
        ') AS "t2" \n' +
        'WHERE "t1".<%= qu(partitioning_column_name) %> = "t2".<%= qu(partitioning_column_name) %> \n' +
        'OR ("t1".<%= qu(partitioning_column_name) %> IS NULL AND "t2".<%= qu(partitioning_column_name) %> IS NULL);'
    ,{imports:{qu:helpers.quoteDotSep}})(args);
};
