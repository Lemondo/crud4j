package com.lemondo.commons.db;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashAPIModel extends HashModel {

	private ProcMetaData insertApi;
	private ProcMetaData updateApi;
	private ProcMetaData deleteApi;
	private ProcMetaData readApi;
	private ProcMetaData listApi;

	public HashAPIModel(Helper helper) {
		super(null, helper);
	}

	public HashAPIModel(TableMetaData meta, Helper helper) {
		super(meta, helper);
	}

	public void setInsertApi(ProcMetaData insertApi) {
		this.insertApi = insertApi;
	}

	public void setUpdateApi(ProcMetaData updateApi) {
		this.updateApi = updateApi;
	}

	public void setDeleteApi(ProcMetaData deleteApi) {
		this.deleteApi = deleteApi;
	}

	public void setReadApi(ProcMetaData readApi) {
		this.readApi = readApi;
	}

	public void setListApi(ProcMetaData listApi) {
		this.listApi = listApi;
	}

	private CallableStatement prepareAPIProc(ProcMetaData proc, Map<String, Object> args) throws SQLException {
		CallableStatement stmnt = helper.prepareCall(proc.genProcedureCall());

		int startInd = 1;

		if (proc.getReturnType() != null) {
			stmnt.registerOutParameter(startInd++, proc.getReturnType());
		}

		List<ProcParam> params = proc.getParamDef();

		for (int i = 0; i < params.size(); i++) {
			Object val = args.get(params.get(i).getName());
			if (val != null) {
				stmnt.setObject(startInd + i, val, params.get(i).getType());
			} else {
				stmnt.setNull(startInd + i, params.get(i).getType());
			}
		}

		return stmnt;
	}

	private int callAPIProc(ProcMetaData proc, Map<String, Object> args) throws SQLException {
		CallableStatement stmnt = prepareAPIProc(proc, args);
		stmnt.execute();

		if (proc.getReturnType() != null && proc.getReturnType() == Types.INTEGER) {
			return stmnt.getInt(1);
		} else {
			return 1;
		}
	}

	private ResultSet queryAPIProc(ProcMetaData proc, Map<String, Object> args) throws SQLException {
		return prepareAPIProc(proc, args).executeQuery();
	}

	@Override
	public int create(String key, Map<String, Object> body) {
		if (insertApi != null) {
			try {
				Map<String, Object> args = new HashMap<String, Object>(body);
				args.put("key", key);
				return callAPIProc(insertApi, args);
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
				return callAPIProc(updateApi, args);
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
				return callAPIProc(deleteApi, args);
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
				ResultSet rs = queryAPIProc(readApi, args);
				if (rs.next()) {
					ResultSetMetaData rsmd = rs.getMetaData();
					return getRowAsMap(rs, rsmd, rsmd.getColumnCount());
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
				ResultSet rs = queryAPIProc(listApi, options);
				ResultSetMetaData rsmd = rs.getMetaData();
				int numColumns = rsmd.getColumnCount();

				List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
				while (rs.next()) {
					result.add(getRowAsMap(rs, rsmd, numColumns));
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
				ResultSet rs = queryAPIProc(listApi, options);
				ResultSetMetaData rsmd = rs.getMetaData();
				int numColumns = rsmd.getColumnCount();

				ObjectOutputStream oOut = new ObjectOutputStream(out);
				while (rs.next()) {
					oOut.writeObject(getRowAsMap(rs, rsmd, numColumns));
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
