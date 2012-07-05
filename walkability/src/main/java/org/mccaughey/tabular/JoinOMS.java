package org.mccaughey.tabular;

import groovy.sql.GroovyResultSetExtension;
import groovy.sql.Sql;

import java.net.URL;

import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Out;

import org.codehaus.groovy.runtime.MethodClosure;
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
			db.execute("create table testA as select * from csvread('"+tableA.toString()+"')");
			db.execute("create table testB as select * from csvread('"+tableB.toString()+"')");
			db.execute("CALL csvwrite('"+dataStore.toString()+"','select * from testA join testB on testA.a = testB.a')");
			db.eachRow("select * from testA join testB on testA.a = testB.a",new MethodClosure(this,"printRow"));
		} catch (Exception e) {
			LOGGER.error("Failed to create in memory database: {}", e.getMessage());
		}
		result = dataStore;
	}
	
	private void printRow(GroovyResultSetExtension row) {
		
		LOGGER.info(row.toString());
	}
}
