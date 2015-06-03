var knex = require('knex')({
	client: 'pg',
	connection: process.env.PG_CONN_STR
});

knex.select().from('mock_data1')
	.then(rows)
console.log(res);

process.exit();