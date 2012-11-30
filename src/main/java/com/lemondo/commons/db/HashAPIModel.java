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

public class HashApiModel extends HashModel {

	private Procedure insertApi;
	private Procedure updateApi;
	private Procedure deleteApi;
	private Procedure readApi;
	private Procedure listApi;

	public HashApiModel(Helper helper) {
		super(null, helper);
	}

	public HashApiModel(TableMetaData meta, Helper helper) {
		super(meta, helper);
	}

	public void setInsertApi(ProcMetaData insertMetaData) {
		this.insertApi = new Procedure(insertMetaData, helper);
	}

	public void setUpdateApi(ProcMetaData updateMetaData) {
		this.updateApi = new Procedure(updateMetaData, helper);
	}

	public void setDeleteApi(ProcMetaData deleteMetaData) {
		this.deleteApi = new Procedure(deleteMetaData, helper);
	}

	public void setReadApi(ProcMetaData readMetaData) {
		this.readApi = new Procedure(readMetaData, helper);
	}

	public void setListApi(ProcMetaData listMetaData) {
		this.listApi = new Procedure(listMetaData, helper);
	}

	@Override
	public int create(String key, Map<String, Object> body) {
		if (insertApi != null) {
			try {
				Map<String, Object> args = new HashMap<String, Object>(body);
				args.put("key", key);
				return insertApi.executeCall(args);
			} catch (SQLException e) {
				throw new RuntimeException("BOOM!", e);
			}
		} else if (metaData != null) {
			return super.create(key, body);
		} else {
			throw new RuntimeException("BOOM!");
		}
	}

	@Override
	public int update(String key, Map<String, Object> body) {
		if (updateApi != null) {
			try {
				Map<String, Object> args = new HashMap<String, Object>(body);
				args.put("key", key);
				return updateApi.executeCall(args);
			} catch (SQLException e) {
				throw new RuntimeException("BOOM!", e);
			}
		} else if (metaData != null) {
			return super.update(key, body);
		} else {
			throw new RuntimeException("BOOM!");
		}
	}

	@Override
	public int delete(String key) {
		if (deleteApi != null) {
			try {
				Map<String, Object> args = new HashMap<String, Object>();
				args.put("key", key);
				return deleteApi.executeCall(args);
			} catch (SQLException e) {
				throw new RuntimeException("BOOM!", e);
			}
		} else if (metaData != null) {
			return super.delete(key);
		} else {
			throw new RuntimeException("BOOM!");
		}
	}

	@Override
	public Map<String, Object> read(String key) {
		if (readApi != null) {
			try {
				Map<String, Object> args = new HashMap<String, Object>();
				args.put("key", key);
				ResultSet rs = readApi.executeQuery(args);
				if (rs.next()) {
					ResultSetMetaData rsmd = rs.getMetaData();
					return readRow(rs, rsmd, rsmd.getColumnCount());
				} else {
					throw new RuntimeException("BOOM: No data found");
				}
			} catch (SQLException e) {
				throw new RuntimeException("BOOM!", e);
			}
		} else if (metaData != null) {
			return super.read(key);
		} else {
			throw new RuntimeException("BOOM!");
		}
	}

	@Override
	public List<Map<String, Object>> list(Map<String, Object> options) {
		if (listApi != null) {
			try {
				ResultSet rs = listApi.executeQuery(options);
				ResultSetMetaData rsmd = rs.getMetaData();
				int numColumns = rsmd.getColumnCount();

				List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
				while (rs.next()) {
					result.add(readRow(rs, rsmd, numColumns));
				}
				return result;
			} catch (SQLException e) {
				throw new RuntimeException("BOOM!", e);
			}
		} else if (metaData != null) {
			return super.list(options);
		} else {
			throw new RuntimeException("BOOM!");
		}
	}

	@Override
	public void list(OutputStream out, Map<String, Object> options) {
		if (listApi != null) {
			try {
				ResultSet rs = listApi.executeQuery(options);
				ResultSetMetaData rsmd = rs.getMetaData();
				int numColumns = rsmd.getColumnCount();

				ObjectOutputStream oOut = new ObjectOutputStream(out);
				while (rs.next()) {
					oOut.writeObject(readRow(rs, rsmd, numColumns));
					oOut.flush();
				}
			} catch (SQLException e) {
				throw new RuntimeException("BOOM!", e);
			} catch (IOException e) {
				throw new RuntimeException("BOOM!", e);
			}
		} else if (metaData != null) {
			super.list(options);
		} else {
			throw new RuntimeException("BOOM!");
		}
	}

}
