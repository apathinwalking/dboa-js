var Promise = require('bluebird');
var _ = require('lodash');
var helpers = require("../helpers.js");
var soloTableQuery = require('./query/solo.js');
var multTableQuery = require('./query/mult.js');

function Table(tblObj) {
  var _this = this;
  _this._tblObj = tblObj;
  _.forIn(soloTableQuery, function(value, key) {
    _this[key] = _.wrap(_this._tblObj, value);
  });
}

function Tables(tblObjs) {
  var _this = this;
  _this._tblObjs = tblObjs;
  _.forIn(soloTableQuery, function(value, key) {
    var func = function(args) {
      var promises = _.map(_.range(tblObjs.length), function(i) {
        if (args === undefined) {
          return value(tblObjs[i]);
        } else if (!Array.isArray(args)) {
          return value(tblObjs[i], args);
        } else {
          return value(tblObjs[i], args[i]);
        }
      });
      return Promise.all(promises);
    };
    _this[key] = func;
  });
  _.forIn(multTableQuery, function(value, key) {
    _this[key] = _.wrap(_this._tblObjs, value);
  });
}

module.exports = function(args) {
  if (Array.isArray(args)) {
    return new Tables(args);
  } else {
    return new Table(args);
  }
};
