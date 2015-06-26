var Promise = require('bluebird');
var column = require('./comparisons/column.js');
//Promise.promisifyAll(column);

var comparisons = {
    "column" : column
};

module.exports= comparisons;
