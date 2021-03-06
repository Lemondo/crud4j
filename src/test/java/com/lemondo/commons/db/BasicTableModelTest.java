package com.lemondo.commons.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.dbunit.Assertion;
import org.dbunit.DatabaseTestCase;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlDataSet;

import com.lemondo.commons.db.meta.TableMetaData;

public class BasicTableModelTest extends DatabaseTestCase {
	private static final String LOGIN_PROPERTIES_FILE = "src/test/login.properties";

	private final String driverName;
	private final String uri;
	private final String userName;
	private final String password;

	private final String inputDataXml;

	private final Helper helper;
	private final TableMetaData testTable01MetaData;
	private final TableMetaData testTable02MetaData;
	private final TableMetaData test2MetaData;

	public BasicTableModelTest() throws FileNotFoundException, IOException, ClassNotFoundException {
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
		this.testTable01MetaData = new TableMetaData("test_table", columnDef, PrimarykeyType.VARCHAR, true);
		this.testTable02MetaData = new TableMetaData("test_table", columnDef, PrimarykeyType.VARCHAR, false);

		HashMap<String, Integer> columnDef2 = new HashMap<String, Integer>();
		columnDef2.put("data", Types.VARCHAR);
		this.test2MetaData = new TableMetaData("test2", columnDef2, PrimarykeyType.INTEGER, false);
	}

	@Override
	protected IDatabaseConnection getConnection() throws Exception {
		return new DatabaseConnection(DriverManager.getConnection(uri, userName, password));
	}

	@Override
	protected IDataSet getDataSet() throws Exception {
		return new FlatXmlDataSet(new FileInputStream(inputDataXml));
	}

	public void testCreate() throws Exception {
		BasicTableModel m = new BasicTableModel(testTable01MetaData, helper);

		HashMap<String, Object> body = new HashMap<String, Object>();
		body.put("empcode", 11);
		body.put("loginname", "obi_wan_kenobi");
		body.put("password", "supersecretpassword");
		body.put("loginenabled", "y");

		m.create("E011", body);

		IDataSet actual = getConnection().createDataSet(new String[] { "test_table" });
		IDataSet expected = new FlatXmlDataSet(new FileInputStream("src/test/data/out-testCreate.xml"));
		Assertion.assertEquals(expected, actual);

		try {
			m.create("E011", body);
			fail("Should throw an exception when trying to insert duplicate entry");
		} catch (Exception e) {
		}
	}

	public void testCreateWithAutoKey() throws Exception {
		BasicTableModel m = new BasicTableModel(test2MetaData, helper);

		HashMap<String, Object> body = new HashMap<String, Object>();
		body.put("data", "foo");

		int id = (Integer) m.create(body);

		ITable actual = getConnection().createQueryTable("test2", "SELECT `data` FROM `test2` WHERE `id` = '" + id + "'");
		ITable expected = new FlatXmlDataSet(new FileInputStream("src/test/data/out-testCreate_with_auto_key.xml")).getTable("test2");
		Assertion.assertEquals(expected, actual);
	}

	public void testUpdate() throws Exception {
		BasicTableModel m = new BasicTableModel(testTable01MetaData, helper);

		HashMap<String, Object> body = new HashMap<String, Object>();
		body.put("loginname", "obi_wan_kenobi");
		body.put("password", "supersecretpassword");

		assertEquals(1, m.update("E001", body));

		IDataSet actual = getConnection().createDataSet(new String[] { "test_table" });
		IDataSet expected = new FlatXmlDataSet(new FileInputStream("src/test/data/out-testUpdate.xml"));
		Assertion.assertEquals(expected, actual);

		assertEquals(0, m.update("E011", body));
	}

	public void testDeleteWithDeactivatedFlag() throws Exception {
		BasicTableModel m = new BasicTableModel(testTable01MetaData, helper);

		assertEquals(1, m.delete("E999"));

		IDataSet actual = getConnection().createDataSet(new String[] { "test_table" });
		IDataSet expected = new FlatXmlDataSet(new FileInputStream("src/test/data/out-testDeleteWithDeactivatedFlag.xml"));
		Assertion.assertEquals(expected, actual);

		assertEquals(0, m.delete("E011"));
	}

	public void testDeleteWithoutDeactivatedFlag() throws Exception {
		BasicTableModel m = new BasicTableModel(testTable02MetaData, helper);

		assertEquals(1, m.delete("E999"));

		IDataSet actual = getConnection().createDataSet(new String[] { "test_table" });
		IDataSet expected = new FlatXmlDataSet(new FileInputStream("src/test/data/out-testDeleteWithoutDeactivatedFlag.xml"));
		Assertion.assertEquals(expected, actual);
	}

	public void testRead() throws Exception {
		BasicTableModel m = new BasicTableModel(testTable01MetaData, helper);

		Map<String, Object> result = m.read("E001");

		assertEquals("E001", (String) result.get("id"));
		assertEquals(new Integer(1), (Integer) result.get("empcode"));
		assertEquals("foo", (String) result.get("loginname"));
		assertEquals("bar", (String) result.get("password"));
		assertEquals("y", (String) result.get("loginenabled"));

		try {
			m.read("D001");
			fail("Should throw an exception when trying to get deleted entry");
		} catch (Exception e) {
		}

		try {
			m.read("E011");
			fail("Should throw an exception when trying to get non existent entry");
		} catch (Exception e) {
		}
	}

	public void testListAsListOfMap() throws Exception {
		BasicTableModel m = new BasicTableModel(testTable01MetaData, helper);

		Map<String, Object> options = new HashMap<String, Object>();
		List<String> sortFields = new ArrayList<String>();
		sortFields.add("loginenabled DESC");
		sortFields.add("loginname");
		options.put("order", sortFields);

		List<Map<String, Object>> result = m.list(options);

		Map<String, Object> row0 = result.get(0);
		assertEquals("E001", row0.get("id"));
		assertEquals(new Integer(1), (Integer) row0.get("empcode"));
		assertEquals("foo", row0.get("loginname"));
		assertEquals("bar", row0.get("password"));
		assertEquals("y", row0.get("loginenabled"));

		Map<String, Object> row1 = result.get(1);
		assertEquals("E999", row1.get("id"));
		assertEquals(new Integer(999999), (Integer) row1.get("empcode"));
		assertEquals("baz", row1.get("loginname"));
		assertEquals("quz", row1.get("password"));
		assertEquals("n", row1.get("loginenabled"));

		options = new HashMap<String, Object>();
		Map<String, Object> filter = new HashMap<String, Object>();
		filter.put("empcode", 999999);
		options.put("filter", filter);

		result = m.list(options);

		assertEquals(1, result.size());
		Map<String, Object> row = result.get(0);
		assertEquals("E999", row.get("id"));
		assertEquals(new Integer(999999), (Integer) row.get("empcode"));
		assertEquals("baz", row.get("loginname"));
		assertEquals("quz", row.get("password"));
		assertEquals("n", row.get("loginenabled"));
	}

	// TODO: implement testListInOutputStream
	// public void testListInOutputStream() throws Exception {
	// fail("Not yet implemented");
	// }

}
