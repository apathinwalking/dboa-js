var _ = require('lodash');
var columnSetCompare = require('./compare/column-set.js');

var matcher =  function(rows,unmatched1,unmatched2){
    var init = {};
    var rows = rows;
    var matched = [];
    var unmatched1 = unmatched1;
    var unmatched2 = unmatched2;

    init.logUnmatched = function(){
        _.forEach(unmatched1,function(r){
            console.log("(" + r.table() + ", "  + r.name() + ")");
        });
        _.forEach(unmatched2,function(r){
            console.log("(" + r.table() + ", "  + r.name() + ")");
        });
        return this;
    };

    init.logRows = function(){
        _.forEach(rows,function(r){
            console.log("(" + r.columns.column1.table() + ", "  + r.columns.column1.name() + "), " + "(" + r.columns.column2.table() + ", "  + r.columns.column2.name() + "), " + r.value);
        });
        return this;
    };

    init.logMatched = function(){
        _.forEach(matched,function(r){
            console.log("(" + r.columns.column1.table() + ", "  + r.columns.column1.name() + "), " + "(" + r.columns.column2.table() + ", "  + r.columns.column2.name() + "), " + r.value);
        });
        return this;
    };

    init.whereEquals = function(value){
        _.forEach(unmatched1, function(u1){
            var match = _.find(rows,function(r){
                return (r.columns.column1 === u1 && r.value === value)
            });
            if(match !== undefined){
                console.log(match);
                matched.push(match);
                unmatched2 = _.filter(unmatched2, function(u2){
                    return u2 !== match.columns.column2;
                });
                rows = _.filter(rows,function(r){
                    return r.columns.column1 != match.columns.column1 && r.columns.column2 != match.columns.column2;
                });
            }
        });

        unmatched1 = _.filter(unmatched1, function(u1){
            return !_.some(matched, function(m){
                return m.columns.column1 === u1;
            });
        });

        _.forEach(unmatched2, function(u2){
            var match = _.find(rows,function(r){
                return (r.columns.column1 === u2 && r.value === value)
            });
            if(match !== undefined){
                matched.push(match);
                unmatched1 = _.filter(unmatched2, function(u1){
                    return u1 !== match.columns.column2;
                });
                rows = _.filter(rows,function(r){
                    return r.columns.column1 != match.columns.column1 && r.columns.column2 != match.columns.column2;
                });
            }
        });

        unmatched2 = _.filter(unmatched2, function(u2){
            return !_.some(matched, function(m){
                return m.columns.column2 === u2;
            });
        });
        return this;
    };

    init.whereGreater = function(value){
        _.forEach(unmatched1, function(u1){
            var match = _.find(rows,function(r){
                return (r.columns.column1 === u1 && r.value > value)
            });
            if(match !== undefined){
                matched.push(match);
                unmatched2 = _.filter(unmatched2, function(u2){
                    return u2 !== match.columns.column2;
                });
                rows = _.filter(rows,function(r){
                    return r.columns.column1 != match.columns.column1 && r.columns.column2 != match.columns.column2;
                });
            }
        });

        unmatched1 = _.filter(unmatched1, function(u1){
            return !_.some(matched, function(m){
                return m.columns.column1 === u1;
            });
        });

        _.forEach(unmatched2, function(u2){
            var match = _.find(rows,function(r){
                return (r.columns.column1 === u2 && r.value > value)
            });
            if(match !== undefined){
                matched.push(match);
                unmatched1 = _.filter(unmatched2, function(u1){
                    return u1 !== match.columns.column2;
                });
                rows = _.filter(rows,function(r){
                    return r.columns.column1 != match.columns.column1 && r.columns.column2 != match.columns.column2;
                });
            }
        });

        unmatched2 = _.filter(unmatched2, function(u2){
            return !_.some(matched, function(m){
                return m.columns.column2 === u2;
            });
        });
        return this;
    };

    init.whereLessThan = function(value){
        _.forEach(unmatched1, function(u1){
            var match = _.find(rows,function(r){
                return (r.columns.column1 === u1 && r.value < value)
            });
            if(match !== undefined){
                matched.push(match);
                unmatched2 = _.filter(unmatched2, function(u2){
                    return u2 !== match.columns.column2;
                });
                rows = _.filter(rows,function(r){
                    return r.columns.column1 != match.columns.column1 && r.columns.column2 != match.columns.column2;
                });
            }
        });

        unmatched1 = _.filter(unmatched1, function(u1){
            return !_.some(matched, function(m){
                return m.columns.column1 === u1;
            });
        });

        _.forEach(unmatched2, function(u2){
            var match = _.find(rows,function(r){
                return (r.columns.column1 === u2 && r.value < value)
            });
            if(match !== undefined){
                matched.push(match);
                unmatched1 = _.filter(unmatched2, function(u1){
                    return u1 !== match.columns.column2;
                });
                rows = _.filter(rows,function(r){
                    return r.columns.column1 != match.columns.column1 && r.columns.column2 != match.columns.column2;
                });
            }
        });

        unmatched2 = _.filter(unmatched2, function(u2){
            return !_.some(matched, function(m){
                return m.columns.column2 === u2;
            });
        });
        return this;
    };

    init.matched = function(){
        return matched;
    };

    init.unmatched1 = function(){
        return unmatched1;
    };

    init.unmatched2 = function(){
        return unmatched2;
    };
    return init;
};



module.exports = matcher;