package com.lemondo.commons.db;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

public class ProcMetaDataTest extends TestCase {

	public ProcMetaDataTest(String name) {
		super(name);
	}

	public void testGenProcedureCall() {
		List<ProcParam> params = new ArrayList<ProcParam>();
		params.add(new ProcParam("foo", Types.VARCHAR));
		params.add(new ProcParam("bar", Types.VARCHAR));

		ProcMetaData p = new ProcMetaData("test_proc", params);

		assertEquals("CALL `test_proc`(?,?)", p.genProcedureCall());
	}

	public void testGenFunctionCall() {
		List<ProcParam> params = new ArrayList<ProcParam>();
		params.add(new ProcParam("foo", Types.VARCHAR));
		params.add(new ProcParam("bar", Types.VARCHAR));

		ProcMetaData p = new ProcMetaData("test_proc", params, Types.INTEGER);

		assertEquals("{?=CALL `test_proc`(?,?)}", p.genProcedureCall());
	}

}
