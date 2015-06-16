var Query = require('./query.js').Query;

module.exports = function(knex){
    this.query = new Query(knex);
};



