"use strict";
var Promise = require('bluebird');
var _ = require('lodash');
var compare = require('./compare.js');
function Dboa(dbConfig){
    var self = this;
    global.knex = require('knex')(dbConfig);
    global.bookshelf = require('bookshelf')(knex);
}

Dboa.prototype = _.assign(
    Dboa.prototype,
    {"compare":compare}
);

module.exports = function(dbConfig){
    return new Dboa(dbConfig);
};

