package com.lemondo.commons.db;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONWriter;

public class JsonModel extends TableModel<JSONObject, JSONArray> {

	public JsonModel(TableMetaData meta, Helper helper) {
		super(meta, helper);
	}

	@Override
	protected Map<String, Object> bodyAsMap(JSONObject body) {
		Map<String, Object> result = new HashMap<String, Object>();

		try {
			@SuppressWarnings("unchecked")
			Iterator<String> keys = body.keys();
			while (keys.hasNext()) {
				String key = keys.next();
				Object val = body.get(key);
				if (val instanceof JSONObject) {
					result.put(key, bodyAsMap((JSONObject) val));
				} else {
					result.put(key, val);
				}
			}
		} catch (JSONException e) {
			throw new RuntimeException("BOOM!", e);
		}

		return result;
	}

	@Override
	protected JSONObject readRow(ResultSet rs, ResultSetMetaData rsmd, int numColumns) throws SQLException {
		try {
			JSONObject result = new JSONObject();
			for (int i = 1; i <= numColumns; i++) {
				result.put(rsmd.getColumnName(i), rs.getObject(i));
			}
			return result;
		} catch (JSONException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

	@Override
	protected JSONArray readAll(ResultSet rs, ResultSetMetaData rsmd, int numColumns) throws SQLException {
		JSONArray result = new JSONArray();
		while (rs.next()) {
			result.put(readRow(rs, rsmd, numColumns));
		}
		return result;
	}

	@Override
	protected void writeRows(OutputStream out, ResultSet rs, ResultSetMetaData rsmd, int numColumns) throws SQLException {
		OutputStreamWriter ow = new OutputStreamWriter(out);
		JSONWriter jw = new JSONWriter(ow);

		try {
			jw.array();
			while (rs.next()) {
				jw.object();
				for (int i = 1; i <= numColumns; i++) {
					jw.key(rsmd.getColumnName(i)).value(rs.getObject(i));
				}
				jw.endObject();
				ow.flush();
			}
			jw.endArray();
			ow.flush();
			ow.close();
		} catch (JSONException e) {
			throw new RuntimeException("BOOM!", e);
		} catch (IOException e) {
			throw new RuntimeException("BOOM!", e);
		}

	}

}
