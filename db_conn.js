const mysql = require('mysql');
let util = require('util');

//https://medium.com/@mhagemann/create-a-mysql-database-middleware-with-node-js-8-and-async-await-6984a09d49f4
let pool = mysql.createPool({
    connectionLimit: 1000,
    host: 'localhost',
    user: 'moripesh_binlogger',
    password: 'Hf6rRKFY',
    database: 'moripesh_payments_live',
});

pool.getConnection((err, connection) => {
    if (err) {
        if (err.code === 'PROTOCOL_CONNECTION_LOST') {
            console.error('Database connection was closed.')
        }
        if (err.code === 'ER_CON_COUNT_ERROR') {
            console.error('Database has too many connections.')
        }
        if (err.code === 'ECONNREFUSED') {
            console.error('Database connection was refused.')
        }
    }
    if (connection) connection.release();
    return
})

pool.query = util.promisify(pool.query);

module.exports = pool;