package com.lemondo.commons.db;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashModel extends TableModel<Map<String, Object>, List<Map<String, Object>>> {

	public HashModel(TableMetaData meta, Helper helper) {
		super(meta, helper);
	}

	@Override
	protected Map<String, Object> bodyAsMap(Map<String, Object> body) {
		return body;
	}

	@Override
	protected Map<String, Object> readRow(ResultSet rs, ResultSetMetaData rsmd, int numColumns) throws SQLException {
		Map<String, Object> result = new HashMap<String, Object>();
		for (int i = 1; i <= numColumns; i++) {
			result.put(rsmd.getColumnName(i), rs.getObject(i));
		}
		return result;
	}

	@Override
	protected List<Map<String, Object>> readAll(ResultSet rs, ResultSetMetaData rsmd, int numColumns) throws SQLException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		while (rs.next()) {
			result.add(readRow(rs, rsmd, numColumns));
		}
		return result;
	}

	@Override
	protected void writeRows(OutputStream out, ResultSet rs, ResultSetMetaData rsmd, int numColumns) throws SQLException {
		try {
			ObjectOutputStream oOut = new ObjectOutputStream(out);
			while (rs.next()) {
				oOut.writeObject(readRow(rs, rsmd, numColumns));
				oOut.flush();
			}
		} catch (IOException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

}
