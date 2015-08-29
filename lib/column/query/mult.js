var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../../helpers.js");
var soloColumnQuery = require('./solo.js');

exports.dataTypesMatch = function(colObjs){
  var promises = _.map(colObjs,function(c){
    return soloColumnQuery.dataType(c);
  });
  return Promise.all(promises)
    .then(function(results){
      if (!Array.isArray(results)){return true;}
      else{
        //all match
        if(_.intersection(results).length === 1){
          return true;
        }
        else{
          return false;
        }
      }
    });
};

exports.getCompatibleDataTypes = function(colObjs){
  var promises = _.map(colObjs,function(c){
    return soloColumnQuery.udtName(c);
  });
  return Promise.all(promises)
    .then(function(results){
      if (!Array.isArray(results)){return results;}
      else{
        //all match
        if(_.intersection(results).length === 1){
          return results[0];
        }
        else{
          return 'text';
        }
      }
    });
};
