
var sql = require('node-sqlserver');
var mssql = require("./lib/mssql");

module.exports.connect = function(connectionString, callback) {
	
	
	sql.open( connectionString, function( err, conn ) {
	
				
  		var db = new mssql(conn);

  		if (callback) { callback(err, db); }
  		

	});
	
};
