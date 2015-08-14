var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");
var query = require("./query.js");

function Column(colObj) {
  var _this = this;
  _this._colObj = colObj;
  _.forIn(query.column, function(value, key) {
    _this[key] = _.wrap(_this._colObj, value);
  });
}

function Columns(colObjs) {
  var _this = this;
  _this._colObjs = colObjs;
  _.forIn(query.column, function(value, key) {
    var func = function(args) {
      var promises = _.map(_.range(colObjs.length), function(i) {
        if (args === undefined) {
          return value(colObjs[i]);
        } else if (!Array.isArray(args)) {
          return value(colObjs[i], args);
        } else {
          return value(colObjs[i], args[i]);
        }
      });
      return Promise.all(promises);
    };
    _this[key] = func;
  });
  _.forIn(query.columns, function(value, key){
    _this[key] = _.wrap(_this._colObjs, value);
  });
}

// _.forIn(query.tables, function(value, key) {
//   _this[key] = _.wrap(_this._tblObjs, value);
// });


function ColumnsSets(colObjsSets) {
  var _this = this;
  _this._colObjsSets = colObjsSets;
  _.forIn(query.column, function(value, key) {
    var func = function(args) {
      var promises = _.map(_.range(colObjsSets.length), function(i) {
        var morePromises = _.map(_.range(colObjsSets[i].length), function(j) {
          if (args === undefined) {
            return value(colObjsSets[i][j]);
          } else if (!Array.isArray(args))  {
            return value(colObjsSets[i][j], args);
          } else if (!Array.isArray(args[i])) {
            return value(colObjsSets[i][j], args[i]);
          } else {
            return value(colObjsSets[i][j], args[i][j]);
          }
        });
        return Promise.all(morePromises);
      });
      return Promise.all(promises);
    };
    _this[key] = func;
  });
  _.forIn(query.columns,function(value,key){
    var func = function(args){
      var promises = _.map(_.range(colObjsSets.length),function(i){
        if(args === undefined) {
          return value(colObjsSets[i]);
        } else if(!Array.isArray(args)) {
          return value(colObjsSets[i], args);
        } else {
          return value(colObjsSets, args[i]);
        }
      });
      return Promise.all(promises);
    };
    _this[key] = func;
  });
}

module.exports = function(args) {
  if (Array.isArray(args)) {
    //TODO: find a better way to do this
    if (Array.isArray(args[0])) {
      return new ColumnsSets(args);
    } else {
      return new Columns(args);
    }
  }
  return new Column(args);
};
