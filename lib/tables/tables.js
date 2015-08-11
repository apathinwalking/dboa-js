var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");
var query = require("./query.js");

function Tables(knex,fq_table_names){
  var _this = this;
  _.forIn(query,function(value,key){
      var q =  _.wrap(knex,value);
      _this[key] = helpers.recursiveWrapSpread(fq_table_names,q);
  });
}

module.exports = function(knex /** table_column_name_pairs**/){
  var argsArr = _.values(arguments);
  var arrArgsArr = _.map(argsArr, function(a){return [a];});
  return new Tables(knex,_.rest(arrArgsArr));
};
