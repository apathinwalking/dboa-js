var Q = require('q');
var _ = require('lodash');

exports.parseTableId = function(currentSchema,tableId){
    "use strict";
    var split = tableId.split('.');
    if(split.length === 1){
        return{'tableName':tableId,'schemaName':currentSchema};
    }
    else{
        return{'tableName':split[1],'schemaName':split[0]};
    }
};

exports.permuteOn = function(arr,property){
    "use strict";
    var propVals = _.uniq(_.pluck(arr,property));
    var permuted = _.reduce(propVals,function(results,p){
        var selected = _.filter(arr,property,p);
        var rest = _.reject(arr,property,p);
        _.forEach(selected,function(s){
            _.forEach(rest,function(r){
                var m = _.merge({},s, function(a,b){return[b];});
                m = _.merge(m,r,function(a,b){return a.concat(b);});
                results.push(m);
            });
        });
        return results;
    },[]);
    return permuted;
};

