var Query = require('./query.js').Query;

module.exports = function(knex){
    "use strict";
    this.query = new Query(knex);
};