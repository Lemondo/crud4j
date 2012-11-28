package com.lemondo.commons.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.dbunit.Assertion;
import org.dbunit.DatabaseTestCase;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;

public class HashAPIModelTest extends DatabaseTestCase {
	private static final String LOGIN_PROPERTIES_FILE = "src/test/login.properties";

	private final String driverName;
	private final String uri;
	private final String userName;
	private final String password;

	private final String inputDataXml;

	private IDataSet loadedDataSet;

	private final Helper helper;

	public HashAPIModelTest() throws FileNotFoundException, IOException, ClassNotFoundException {
		Properties prop = new Properties();
		prop.load(new FileInputStream(LOGIN_PROPERTIES_FILE));

		driverName = prop.getProperty("com.lemondo.commons.db.test.DriverName");
		uri = prop.getProperty("com.lemondo.commons.db.test.URI");
		userName = prop.getProperty("com.lemondo.commons.db.test.User");
		password = prop.getProperty("com.lemondo.commons.db.test.Password");

		inputDataXml = prop.getProperty("com.lemondo.commons.db.test.InputDataSet");

		Class.forName(driverName);

		helper = Helper.getInstance(driverName, uri, userName, password);

		HashMap<String, Integer> columnDef = new HashMap<String, Integer>();
		columnDef.put("empcode", Types.INTEGER);
		columnDef.put("loginname", Types.VARCHAR);
		columnDef.put("password", Types.VARCHAR);
		columnDef.put("loginenabled", Types.VARCHAR);
	}

	@Override
	protected IDatabaseConnection getConnection() throws Exception {
		return new DatabaseConnection(DriverManager.getConnection(uri, userName, password));
	}

	@Override
	protected IDataSet getDataSet() throws Exception {
		loadedDataSet = new FlatXmlDataSet(new FileInputStream(inputDataXml));
		return loadedDataSet;
	}

	public void testCreate() throws Exception {
		HashAPIModel m = new HashAPIModel(helper);
		List<ProcParam> params = new ArrayList<ProcParam>();
		params.add(new ProcParam("key", Types.VARCHAR));
		params.add(new ProcParam("empcode", Types.INTEGER));
		params.add(new ProcParam("loginname", Types.VARCHAR));
		params.add(new ProcParam("password", Types.VARCHAR));
		params.add(new ProcParam("loginenabled", Types.VARCHAR));
		m.setInsertApi(new ProcMetaData("ins_test_table", params));

		HashMap<String, Object> body = new HashMap<String, Object>();
		body.put("empcode", 11);
		body.put("loginname", "obi_wan_kenobi");
		body.put("password", "supersecretpassword");
		body.put("__loginenabled", "y");

		assertEquals(1, m.create("E011", body));

		IDataSet actual = getConnection().createDataSet(new String[] { "test_table" });
		IDataSet expected = new FlatXmlDataSet(new FileInputStream("src/test/data/out-api-testCreate.xml"));
		Assertion.assertEquals(expected, actual);

		try {
			m.create("E011", body);
			fail("Should throw an exception when trying to insert duplicate entry");
		} catch (Exception e) {
		}
	}

	public void testUpdate() throws Exception {
		HashAPIModel m = new HashAPIModel(helper);
		List<ProcParam> params = new ArrayList<ProcParam>();
		params.add(new ProcParam("key", Types.VARCHAR));
		params.add(new ProcParam("empcode", Types.INTEGER));
		params.add(new ProcParam("loginname", Types.VARCHAR));
		params.add(new ProcParam("password", Types.VARCHAR));
		params.add(new ProcParam("loginenabled", Types.VARCHAR));
		m.setUpdateApi(new ProcMetaData("upd_test_table", params, Types.INTEGER));

		HashMap<String, Object> body = new HashMap<String, Object>();
		body.put("empcode", 7);
		body.put("loginname", "obi_wan_kenobi");
		body.put("password", "supersecretpassword");

		assertEquals(1, m.update("E001", body));

		IDataSet actual = getConnection().createDataSet(new String[] { "test_table" });
		IDataSet expected = new FlatXmlDataSet(new FileInputStream("src/test/data/out-api-testUpdate.xml"));
		Assertion.assertEquals(expected, actual);

		assertEquals(0, m.update("E011", body));
	}

	public void testDeleteWithoutDeactivatedFlag() throws Exception {
		HashAPIModel m = new HashAPIModel(helper);
		List<ProcParam> params = new ArrayList<ProcParam>();
		params.add(new ProcParam("key", Types.VARCHAR));
		m.setDeleteApi(new ProcMetaData("del_test_table", params, Types.INTEGER));

		assertEquals(1, m.delete("E999"));

		IDataSet actual = getConnection().createDataSet(new String[] { "test_table" });
		IDataSet expected = new FlatXmlDataSet(new FileInputStream("src/test/data/out-testDeleteWithoutDeactivatedFlag.xml"));
		Assertion.assertEquals(expected, actual);
	}

}
