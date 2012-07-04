package org.mccaughey.tabular;

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

	@Out
	URL result;

	@Execute
	public void join() {
		result = tableA;
	}
}
