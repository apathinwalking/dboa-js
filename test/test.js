var dboa = require("../lib/index.js")({
  client: "pg",
  connection: "dbname=poco host=localhost port=5432 user=postgres password='password'"
});
var Promise = require('bluebird');
