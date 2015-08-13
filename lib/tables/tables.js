var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");
var query = require("./query.js");

function Tables(knex,fq_table_names){
  var _this = this;
  var arr_fq_table_names = _.map(fq_table_names, function(a){return [a];});
  _.forIn(query.imported,function(value,key){
      var q =  _.wrap(knex,value);
      _this[key] = helpers.recursiveWrapSpread(arr_fq_table_names,q);
  });
  _.forIn(query.original,function(value,key){
    _this[key] = helpers.recursiveWrap([knex,fq_table_names],value);
  });
}

module.exports = function(knex /** fq_table_names **/){
  var argsArr = _.values(arguments);
  return new Tables(knex,_.rest(argsArr));
};
