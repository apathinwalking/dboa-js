exports.parseTableId = function(currentSchema,tableId){
    "use strict";
    var split = tableId.split('.');
    if(split.length === 1){
        return{'tableName':tableId,'schemaName':currentSchema};
    }
    else{
        return{'tableName':split[1],'schemaName':split[0]};
    }
    throw error;
};

exports.resolveSharedTableNames = function(schemaNames,tableNames,queryResults){
    "use strict";
};