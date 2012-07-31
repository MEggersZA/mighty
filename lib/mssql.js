var events = require('events');
var util = require('util');
var _ = require('underscore')._;
var Query = require('./query');
var Table = require('./table');

var MSSQL = function(connection){

	events.EventEmitter.call(this);
	var self = this;  	
  	this.tables = [];
  	this.conn = connection;

  	this.tableSQL = "SELECT                         \
    table_name as name,                           \
    (select cu.column_name                        \
      from                                        \
         information_schema.key_column_usage cu,  \
         information_schema.table_constraints tc  \
      where                                       \
        cu.constraint_name = tc.constraint_name   \
      and tc.table_name = ist.table_name          \
    ) as pk                                       \
    from information_schema.tables ist";

  	this.placeholder = function(seed) { return '?'; }

  	this.execute = function(sql, params, callback) {

	    self.emit("beforeExecute", self);
	    
	    var stmt = this.conn.query(sql, params, function(err, results) {
	    	
	    	if(err) {
		        if(callback) {callback(err,null);}
		        self.emit("error", err);
			} else {
				
		        if(callback) {callback(null, results);}
		        self.emit("executed");
		    }
	    });	    	    	   
	};

	this.useTable = function(name, pk){

		var t = new Table(name, pk, self);
        self.tables.push(t);
        self[t.name] = t;
	};


	this.run = function(sql, params, callback) {
		
		for(var prop in  params) {
			if (params.hasOwnProperty(prop)) {
				var value = params[prop];
											
				if (_.isDate(value) || _.isString(value)) {
					sql = sql.replace("@" + prop,"'" + value.replace('\'','\'\'') + "'");
					//sql = sql.replace("@" + prop, value);						
				} else {
					sql = sql.replace("@" + prop, value);						
				}
				
			}	
		}
		
		this.execute(sql, [], callback);
	};		

	this.loadTables = function(callback) {
	    self.execute(self.tableSQL, [], function(err, tables){
	      _.each(tables, function(table){
	        var t = new Table(table.name, table.pk, self);
	        self.tables.push(t);
	        self[t.name] = t;
	      });
	      callback(null,self);
	    });
	};
	
	var _translateType = function(typeName) {
  		var _result = typeName;

	  switch(typeName){
	    case "pk" :
	      _result ="INT IDENTITY(1,1) PRIMARY KEY NOT NULL";
	      break;
	    case "money" :
	      _result ="decimal(8,2)";
	      break;
	    case "date" :
	      _result = "datetime";
	      break;
	    case "string" :
	      _result ="varchar(255)";
	      break;
	  }
	  return _result;
	}

	var _containsPK = function(columns) {
	  return _.any(columns.values,"pk");
	}

	this.dropTable = function(tableName) {
	  //return new Query("DROP TABLE IF EXISTS " + tableName + ";", [], new Table(tableName, "", self));
	  return new Query("IF OBJECT_ID('" + tableName + "', 'U') IS NOT NULL DROP TABLE " + tableName , [], new Table(tableName, "", self));  
	}

	this.createTable = function(tableName, columns) {

	  var _sql ="CREATE TABLE " + tableName + "(";
	  var _cols = [];

	  //force a PK
	  if(!_containsPK(columns)){
	    columns.id = "pk";
	  }

	  for(var c in columns){

	    if(c == "timestamps"){
	      _cols.push("created_at datetime NOT NULL");
	      _cols.push("updated_at timestamp NOT NULL");
	    }else{
	      var colName = c;
	      var colParts = columns[c].split(" ");
	      var colType = colParts[0];
	      var translated = _translateType(colType);
	      var extras = _.without(colParts,colType).join(" ");
	      var declaration = colName + " " + translated + " " + extras;
	      //console.log(declaration);
	      _cols.push(declaration);
	    }
	 }

  	_sql+= _cols.join(",") + ");";
  		return new Query(_sql, [], new Table(tableName, columns.id, self));
	};

	this.beginTransaction = function(callback) {

		function onBeginTxn(err) {

			self.emit("beginTransaction", err);

			if( callback ) {
                    callback( err );
			}

		};

		this.conn.beginTransaction(onBeginTxn);
	};
	this.commit = function(callback) {
		this.conn.commit(callback);	
	};
	this.rollback = function(callback) {
		this.conn.rollback(callback);	
	};
	this.close = function (callback) { 
		this.conn.close(callback);		
	};
};

util.inherits(MSSQL, events.EventEmitter);

module.exports = MSSQL;