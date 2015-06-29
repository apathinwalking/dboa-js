var _ = require('lodash');
var Promise = require('bluebird');
var columnSet = require('./column-set.js');
var columnSetCompare = require('./compare/column-set.js');

var compare = {};

compare.tables = function(opt1,opt2){
    return Promise.all([
        columnSet.createColumnSetFromTableId(opt1),
        columnSet.createColumnSetFromTableId(opt2)
    ]).spread(function(columnSet1,columnSet2){
        return columnSetCompare(columnSet1,columnSet2);
    });
};

compare.columnSets = function(opt1,opt2){
    return columnSetCompare(opt1,opt2);
};



module.exports = compare;