package org.mccaughey.tabular;

import groovy.sql.Sql;

import java.net.URL;

import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Out;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoinOMS {
	static final Logger LOGGER = LoggerFactory.getLogger(JoinOMS.class);

	@In
	URL tableA;

	@In
	URL tableB;
	
	@In
	String columnName;

	@In 
	URL dataStore;
	
	@Out
	URL result;

	@Execute
	public void join() {
		try {
			Sql db = Sql.newInstance("jdbc:h2:mem:temp", "org.h2.Driver");
			db.execute(String.format("create table testA as select * from csvread('%s')",tableA.toString()));
			db.execute(String.format("create table testB as select * from csvread('%s')",tableB.toString()));
			db.execute(String.format("CALL csvwrite('%s','select * from testA join testB on testA.a = testB.a')",dataStore));
		//	db.eachRow("select * from testA join testB on testA.a = testB.a",new MethodClosure(this,"printRow"));
		//	db.e
		} catch (Exception e) {
			LOGGER.error("Failed to create in memory database: {}", e.getMessage());
		}
		result = dataStore;
	}
	
	//private void printRow(GroovyResultSetExtension row) {
		
	//	LOGGER.info(row.toString());
	//}
}
