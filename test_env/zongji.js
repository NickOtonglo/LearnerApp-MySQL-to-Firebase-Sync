var ZongJi = require('zongji');
var zongji = new ZongJi({
  host     : 'localhost',
  user     : 'binlogger',
  password : 'admin1234',
  debug: true
});

zongji.on('binlog', function(evt) {
  evt.dump();
});