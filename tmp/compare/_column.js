var _ = require('lodash');
var Promise = require('bluebird');
var leven = require('leven');
var wuzzy = require('wuzzy');

var queries = require('../queries.js');

var column = {};


//returns a count of the number of exact matches found between rows, counting by twos
column._countMatches = function(tableId1,columnName1,tableId2,columnName2,limit){
    return Promise.all([
        queries.column.countDistinct(tableId1,columnName1),
        queries.column.countDistinct(tableId2,columnName2)
    ])
        .spread(function(counts1,counts2){
            if(limit !== undefined){
                if(limit < counts1.length){
                    counts1 = _.sortBy(counts1,'count');
                    counts1 = _.slice(counts1,0,limit);
                }
                if(limit < counts2.length){
                    counts2 = _.sortBy(counts2,'count');
                    counts2 = _.slice(counts2,0,limit);
                }
            }
            var distincts = _.union(_.pluck(counts1,'value'), _.pluck(counts2,'value'));
            var count = _.reduce(distincts, function(result,value){
                var check1 = _.where(counts1,{"value":value});
                var check2 = _.where(counts2,{"value":value});
                if(check1.length > 0 && check2.length > 0){
                    result += parseInt(_.pluck(check1,"count")[0]);
                    result += parseInt(_.pluck(check2,"count")[0]);
                }
                return result;
            },0);
            return count;
        })
};

//returns a ratio of the matches count to the total number of rows in the two tables
column._countMatchesRatio = function(tableId1,columnName1,tableId2,columnName2,limit){
    return Promise.all([
        queries.column.count(tableId1,columnName1),
        queries.column.count(tableId2,columnName2),
        this._countMatches(tableId1,columnName1,tableId2,columnName2,limit)
    ])
        .spread(function(rowCount1,rowCount2,matchCount){
            return matchCount / (rowCount1 + rowCount2);
        })
};
//returns 1 if names match 0 if not
column.name = function(columnName1,columnName2){
    if(columnName1 === columnName2){return 1;}
    else{return 0;}
};

//returns the levenshtein edit distance between the two column names
column.nameEditDistance = function(columnName1,columnName2){
    return leven(columnName1,columnName2);
};

//returns the levenstein edit distance ratio between the two column names
column.nameEditDistanceRatio = function(columnName1, columnName2){
    return wuzzy.levenshtein(columnName1,columnName2);
};

column.type = function(tableId1,columnName1,tableId2,columnName2){
    return Promise.all([
        queries.column.type(tableId1,columnName1),
        queries.column.type(tableId2,columnName2)
    ])
        .spread(function(type1,type2){
            if(type1 === type2){return 1;}
            else{return 0;}
    });
};

module.exports = column;
