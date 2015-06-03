var q = require('q');
var _ = require('lodash');

var Dboa = function(knex){
    //TODO: deal with more complex connection options
    this.knex = knex;
    this.current_schema = 'public';//replaced when initialize is called
};

/** Parses schema (or implied schema) and table name from a fully-qualified table name **/
Dboa.prototype._parseTableId = function(table_id){
    var self = this;
    var spl = table_id.split('.');
    if(spl.length == 1){
        return {'table_name':table_id, 'schema_name':self.current_schema}
    }
    else{
        return {'table_name':spl[1], 'schema_name':spl[0]};
    }
    return false;
};

Dboa.prototype.initialize = function(){
    var self = this;
    var deferred = q.defer();
    //get current_schema
    this.knex.raw('select current_schema()')
        .then(function(response){
            self.current_schema = _.pluck(response.rows,'current_schema')[0];
            deferred.resolve(true);
        });
    return deferred.promise;
};

Dboa.prototype.whichTablesInSchema = function(schema_name){
    var deferred = q.defer();
    this.knex.select('table_name')
        .from('information_schema.tables')
        .where({
            'table_schema':schema_name
        })
        .pluck('table_name')
        .then(function(rows){
            deferred.resolve(rows)
        });
    return deferred.promise;
};

Dboa.prototype.getColumnType = function(table_id,column_name){
    var parsed = this._parseTableId(table_id);
    var deferred = q.defer();
    this.knex.select('data_type')
        .from('information_schema.columns')
        .where('table_schema',parsed.schema_name)
        .andWhere('table_name',parsed.table_name)
        .andWhere('column_name',column_name)
        .pluck(['data_type'])
        .then(function(rows){
            deferred.resolve(rows[0]);
        });
    return deferred.promise;
};

Dboa.prototype.


module.exports = Dboa;

