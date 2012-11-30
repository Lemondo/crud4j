package com.lemondo.commons.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
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
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.lemondo.commons.db.meta.TableMetaData;

public class JsonTableModelTest extends DatabaseTestCase {
	private static final String LOGIN_PROPERTIES_FILE = "src/test/login.properties";

	private final String driverName;
	private final String uri;
	private final String userName;
	private final String password;

	private final String inputDataXml;

	private final Helper helper;
	private final TableMetaData testTable01MetaData;
	private final TableMetaData testTable02MetaData;

	public JsonTableModelTest() throws FileNotFoundException, IOException, ClassNotFoundException {
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
		this.testTable02MetaData = new TableMetaData("test_table", columnDef, false);
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
		JsonTableModel m = new JsonTableModel(testTable01MetaData, helper);

		JSONObject body = new JSONObject();
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
			fail("Should throw an exception when trying to insert duplicate entry");
		} catch (Exception e) {
		}
	}

	public void testUpdate() throws Exception {
		JsonTableModel m = new JsonTableModel(testTable01MetaData, helper);

		JSONObject body = new JSONObject();
		body.put("loginname", "obi_wan_kenobi");
		body.put("password", "supersecretpassword");

		assertEquals(1, m.update("E001", body));

		IDataSet actual = getConnection().createDataSet(new String[] { "test_table" });
		IDataSet expected = new FlatXmlDataSet(new FileInputStream("src/test/data/out-testUpdate.xml"));
		Assertion.assertEquals(expected, actual);

		assertEquals(0, m.update("E011", body));
	}

	public void testDeleteWithDeactivatedFlag() throws Exception {
		JsonTableModel m = new JsonTableModel(testTable01MetaData, helper);

		assertEquals(1, m.delete("E999"));

		IDataSet actual = getConnection().createDataSet(new String[] { "test_table" });
		IDataSet expected = new FlatXmlDataSet(new FileInputStream("src/test/data/out-testDeleteWithDeactivatedFlag.xml"));
		Assertion.assertEquals(expected, actual);

		assertEquals(0, m.delete("E011"));
	}

	public void testDeleteWithoutDeactivatedFlag() throws Exception {
		JsonTableModel m = new JsonTableModel(testTable02MetaData, helper);

		assertEquals(1, m.delete("E999"));

		IDataSet actual = getConnection().createDataSet(new String[] { "test_table" });
		IDataSet expected = new FlatXmlDataSet(new FileInputStream("src/test/data/out-testDeleteWithoutDeactivatedFlag.xml"));
		Assertion.assertEquals(expected, actual);
	}

	public void testRead() throws Exception {
		JsonTableModel m = new JsonTableModel(testTable01MetaData, helper);

		JSONObject result = m.read("E001");

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
		JsonTableModel m = new JsonTableModel(testTable01MetaData, helper);

		Map<String, Object> options = new HashMap<String, Object>();
		List<String> sortFields = new ArrayList<String>();
		sortFields.add("loginenabled DESC");
		sortFields.add("loginname");
		options.put("order", sortFields);

		JSONArray result = m.list(options);

		JSONObject row0 = result.getJSONObject(0);
		assertEquals("E001", row0.get("id"));
		assertEquals(new Integer(1), (Integer) row0.get("empcode"));
		assertEquals("foo", row0.get("loginname"));
		assertEquals("bar", row0.get("password"));
		assertEquals("y", row0.get("loginenabled"));

		JSONObject row1 = result.getJSONObject(1);
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

		assertEquals(1, result.length());
		JSONObject row = result.getJSONObject(0);
		assertEquals("E999", row.get("id"));
		assertEquals(new Integer(999999), (Integer) row.get("empcode"));
		assertEquals("baz", row.get("loginname"));
		assertEquals("quz", row.get("password"));
		assertEquals("n", row.get("loginenabled"));
	}

	public void testListInOutputStream() throws Exception {
		JsonTableModel m = new JsonTableModel(testTable01MetaData, helper);

		Map<String, Object> options = new HashMap<String, Object>();
		List<String> sortFields = new ArrayList<String>();
		sortFields.add("loginenabled DESC");
		sortFields.add("loginname");
		options.put("order", sortFields);

		PipedInputStream in = new PipedInputStream();
		m.list(new PipedOutputStream(in), options);
		InputStreamReader reader = new InputStreamReader(in);
		JSONTokener tokener = new JSONTokener(reader);
		JSONArray result = new JSONArray(tokener);

		JSONObject row0 = result.getJSONObject(0);
		assertEquals("E001", row0.get("id"));
		assertEquals(new Integer(1), (Integer) row0.get("empcode"));
		assertEquals("foo", row0.get("loginname"));
		assertEquals("bar", row0.get("password"));
		assertEquals("y", row0.get("loginenabled"));

		JSONObject row1 = result.getJSONObject(1);
		assertEquals("E999", row1.get("id"));
		assertEquals(new Integer(999999), (Integer) row1.get("empcode"));
		assertEquals("baz", row1.get("loginname"));
		assertEquals("quz", row1.get("password"));
		assertEquals("n", row1.get("loginenabled"));

		options = new HashMap<String, Object>();
		Map<String, Object> filter = new HashMap<String, Object>();
		filter.put("empcode", 999999);
		options.put("filter", filter);

		in = new PipedInputStream();
		m.list(new PipedOutputStream(in), options);
		result = new JSONArray(new JSONTokener(new InputStreamReader(in)));

		assertEquals(1, result.length());
		JSONObject row = result.getJSONObject(0);
		assertEquals("E999", row.get("id"));
		assertEquals(new Integer(999999), (Integer) row.get("empcode"));
		assertEquals("baz", row.get("loginname"));
		assertEquals("quz", row.get("password"));
		assertEquals("n", row.get("loginenabled"));
	}

}
