var Query = require('./query.js').Query;
var Compare = require('./compare.js').Compare;

module.exports = function(knex){
    this.query = new Query(knex);
    this.compare = new Compare(knex);
};