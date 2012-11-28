package com.lemondo.commons.db;

import java.sql.Types;
import java.util.HashMap;

import junit.framework.TestCase;

public class ProcMetaDataTest extends TestCase {

	public ProcMetaDataTest(String name) {
		super(name);
	}

	public void testGenProcedureCall() {
		HashMap<String, Integer> params = new HashMap<String, Integer>();
		params.put("foo", Types.VARCHAR);
		params.put("bar", Types.VARCHAR);

		ProcMetaData p = new ProcMetaData("test_proc", params);

		assertEquals("CALL `test_proc`(?,?)", p.genProcedureCall());
	}

}
