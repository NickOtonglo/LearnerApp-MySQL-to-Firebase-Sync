let mysql = require('mysql');

let con = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'learnerapp'
});

con.connect(function(err){
    if(err) throw err;
    console.log('Connected to database');
    let sql = "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, uid VARCHAR(50), phone VARCHAR(13), email VARCHAR(100), amount DOUBLE(9,2))";
    con.query(sql,function(err,result){
        if (err) throw err;
        console.log('Table created');
    });
});