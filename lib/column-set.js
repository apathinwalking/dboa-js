var _ = require('lodash');
var columnSetCompare = require('./compare/column-set.js');
var createColumn = require('./column.js');
var createColumnSetFromTableId = function(opts) {
    return knex.select('column_name', knex.raw('table_schema || \'.\' || table_name as table_id'))
        .from('information_schema.columns')
        .where(knex.raw('table_schema || \'.\' || table_name'), opts.tableId)
        .then(function (results) {
            return _.map(results,function(r){
                return createColumn({tableId: r.table_id, columnName: r.column_name});
            });
        });
};

exports.createColumnSetFromTableId = createColumnSetFromTableId;
