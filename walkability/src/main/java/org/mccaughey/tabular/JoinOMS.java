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

/**
 * Performs a join on two tables - this should be refactored so it can handle spatial and non-spatial datasets
 * @author amacaulay
 *
 */
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

	/**
	 * Performs a join using and in memory h2 database and groovy sql.
	 */
	@Execute
	public void join() {
		try {
			Sql db = Sql.newInstance("jdbc:h2:mem:temp", "org.h2.Driver");
			db.execute(String.format("create table testA as select * from csvread('%s')",tableA.toString()));
			db.execute(String.format("create table testB as select * from csvread('%s')",tableB.toString()));
			db.execute(String.format("CALL csvwrite('%s','select * from testA join testB on testA.a = testB.a')",dataStore));
			db.eachRow("select * from testA join testB on testA.a = testB.a",printRow(null));
		//	db.e
		} catch (Exception e) {
			LOGGER.error("Failed to create in memory database: {}", e.getMessage());
		}
		result = dataStore;
	}
	
	private MethodClosure printRow(GroovyResultSetExtension row) {
		if (row == null)
			return new MethodClosure(this,"printRow");
		else
			LOGGER.info(row.toString());
		return null;
	}
}
