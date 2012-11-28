package com.lemondo.commons.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.HashMap;
import java.util.Properties;

import org.dbunit.Assertion;
import org.dbunit.DatabaseTestCase;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;

public class HashModelTest extends DatabaseTestCase {
	private static final String LOGIN_PROPERTIES_FILE = "src/test/login.properties";

	private final String driverName;
	private final String uri;
	private final String userName;
	private final String password;

	private final String inputDataXml;

	private IDataSet loadedDataSet;

	private final Helper helper;
	private final TableMetaData testTable01MetaData;

	public HashModelTest() throws FileNotFoundException, IOException, ClassNotFoundException {
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
		this.testTable01MetaData = new TableMetaData("test_table", columnDef, true);
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
		HashModel m = new HashModel(testTable01MetaData, helper);

		HashMap<String, Object> body = new HashMap<String, Object>();
		body.put("empcode", 11);
		body.put("loginname", "obi_wan_kenobi");
		body.put("password", "supersecretpassword");
		body.put("loginenabled", "y");

		assertEquals(1, m.create("E011", body));

		IDataSet actual = getConnection().createDataSet(new String[] { "test_table" });
		IDataSet expected = new FlatXmlDataSet(new FileInputStream("src/test/data/out-testCreate.xml"));

		Assertion.assertEquals(expected, actual);

		try {
			m.create("E011", body);
			fail("Showld throw an exception when trying to insert duplicate entry");
		} catch (Exception e) {
			assertTrue(e instanceof Exception);
		}
	}

	public void testUpdate() throws Exception {
		HashModel m = new HashModel(testTable01MetaData, helper);

		HashMap<String, Object> body = new HashMap<String, Object>();
		body.put("loginname", "obi_wan_kenobi");
		body.put("password", "supersecretpassword");

		assertEquals(1, m.update("E001", body));

		IDataSet actual = getConnection().createDataSet(new String[] { "test_table" });
		IDataSet expected = new FlatXmlDataSet(new FileInputStream("src/test/data/out-testUpdate.xml"));

		Assertion.assertEquals(expected, actual);

		assertEquals(0, m.update("E011", body));
	}

//	public void testDelete() throws Exception {
//		fail("Not yet implemented");
//	}
//
//	public void testRead() throws Exception {
//		fail("Not yet implemented");
//	}
//
//	public void testListMapOfStringObject() throws Exception {
//		fail("Not yet implemented");
//	}
//
//	public void testListOutputStreamMapOfStringObject() throws Exception {
//		fail("Not yet implemented");
//	}

}
