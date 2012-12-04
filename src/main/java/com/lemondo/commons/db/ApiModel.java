package com.lemondo.commons.db;

import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.lemondo.commons.db.exception.DataProcessingException;
import com.lemondo.commons.db.exception.DatabaseOperationException;
import com.lemondo.commons.db.exception.ForeignKeyViolation;
import com.lemondo.commons.db.exception.InvalidFieldException;
import com.lemondo.commons.db.exception.NoDataFoundException;
import com.lemondo.commons.db.exception.NotNullViolation;
import com.lemondo.commons.db.exception.UniqueKeyViolation;
import com.lemondo.commons.db.meta.ProcMetaData;
import com.lemondo.commons.db.meta.TableMetaData;
import com.lemondo.commons.db.processor.DataProcessor;

public class ApiModel<T, L> implements Model<T, L> {

	private final Helper helper;

	private final DataProcessor<T, L> processor;

	private final TableModel<T, L> tableModel;

	private Procedure insertApi;
	private Procedure updateApi;
	private Procedure deleteApi;
	private Procedure readApi;
	private Procedure listApi;

	public ApiModel(Helper helper, DataProcessor<T, L> processor) {
		this.helper = helper;
		this.processor = processor;
		this.tableModel = null;
	}

	public ApiModel(Helper helper, DataProcessor<T, L> processor, TableMetaData meta) {
		this.helper = helper;
		this.processor = processor;
		this.tableModel = new TableModel<T, L>(meta, helper, processor);
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
	public void create(Object key, T body) throws InvalidFieldException, DataProcessingException, DatabaseOperationException {
		if (insertApi != null) {
			try {
				Map<String, Object> args = processor.bodyAsMap(body);
				args.put("key", key);
				insertApi.executeProcedure(args);
			} catch (SQLException e) {
				if (Helper.isNotNullViolation(e)) {
					throw new NotNullViolation("Mandatory field is omitted", e);
				} else if (Helper.isForeignKeyViolation(e)) {
					throw new ForeignKeyViolation("Parent record not found", e);
				} else if (Helper.isUniqueKeyViolation(e)) {
					throw new UniqueKeyViolation("Record with given key already exists", e);
				} else {
					throw new DatabaseOperationException("Error while DB operation", e);
				}
			}
		} else if (tableModel != null) {
			tableModel.create(key, body);
		} else {
			throw new IllegalStateException("ApiModel was not initialized properly");
		}
	}

	@Override
	public Object create(T body) throws InvalidFieldException, DataProcessingException, DatabaseOperationException {
		if (insertApi != null) {
			try {
				Map<String, Object> args = processor.bodyAsMap(body);
				return (String) insertApi.executeFunction(args);
			} catch (SQLException e) {
				if (Helper.isNotNullViolation(e)) {
					throw new NotNullViolation("Mandatory field is omitted", e);
				} else if (Helper.isForeignKeyViolation(e)) {
					throw new ForeignKeyViolation("Parent record not found", e);
				} else {
					throw new DatabaseOperationException("Error while DB operation", e);
				}
			}
		} else if (tableModel != null) {
			return tableModel.create(body);
		} else {
			throw new IllegalStateException("ApiModel was not initialized properly");
		}
	}

	@Override
	public int update(Object key, T body) throws InvalidFieldException, DataProcessingException, DatabaseOperationException {
		if (updateApi != null) {
			try {
				Map<String, Object> args = processor.bodyAsMap(body);
				args.put("key", key);
				return (Integer) updateApi.executeFunction(args);
			} catch (SQLException e) {
				if (Helper.isNotNullViolation(e)) {
					throw new NotNullViolation("Mandatory field is omitted", e);
				} else if (Helper.isForeignKeyViolation(e)) {
					throw new ForeignKeyViolation("Foreign key violation", e);
				} else {
					throw new DatabaseOperationException("Error while DB operation", e);
				}
			}
		} else if (tableModel != null) {
			return tableModel.update(key, body);
		} else {
			throw new IllegalStateException("ApiModel was not initialized properly");
		}
	}

	@Override
	public int delete(Object key) throws DatabaseOperationException {
		if (deleteApi != null) {
			try {
				Map<String, Object> args = new HashMap<String, Object>();
				args.put("key", key);
				return (Integer) deleteApi.executeFunction(args);
			} catch (SQLException e) {
				if (Helper.isForeignKeyViolation(e)) {
					throw new ForeignKeyViolation("Cannot delete, child record found", e);
				} else {
					throw new DatabaseOperationException("Error while DB operation", e);
				}
			}
		} else if (tableModel != null) {
			return tableModel.delete(key);
		} else {
			throw new IllegalStateException("ApiModel was not initialized properly");
		}
	}

	@Override
	public T read(Object key) throws NoDataFoundException, DataProcessingException, DatabaseOperationException {
		if (readApi != null) {
			try {
				Map<String, Object> args = new HashMap<String, Object>();
				args.put("key", key);
				ResultSet rs = readApi.executeQuery(args);
				if (rs.next()) {
					ResultSetMetaData rsmd = rs.getMetaData();
					return processor.readRow(rs, rsmd, rsmd.getColumnCount());
				} else {
					throw new NoDataFoundException("Could not find record with given key");
				}
			} catch (SQLException e) {
				throw new DatabaseOperationException("Error while DB operation", e);
			}
		} else if (tableModel != null) {
			return tableModel.read(key);
		} else {
			throw new IllegalStateException("ApiModel was not initialized properly");
		}
	}

	@Override
	public L list(Map<String, Object> options) throws DataProcessingException, DatabaseOperationException {
		if (listApi != null) {
			try {
				ResultSet rs = listApi.executeQuery(options);
				ResultSetMetaData rsmd = rs.getMetaData();
				int numColumns = rsmd.getColumnCount();

				return processor.readAll(rs, rsmd, numColumns);
			} catch (SQLException e) {
				throw new DatabaseOperationException("Error while DB operation", e);
			}
		} else if (tableModel != null) {
			return tableModel.list(options);
		} else {
			throw new IllegalStateException("ApiModel was not initialized properly");
		}
	}

	@Override
	public void list(OutputStream out, Map<String, Object> options) throws DataProcessingException, DatabaseOperationException {
		if (listApi != null) {
			try {
				ResultSet rs = listApi.executeQuery(options);
				ResultSetMetaData rsmd = rs.getMetaData();
				int numColumns = rsmd.getColumnCount();

				processor.writeRows(out, rs, rsmd, numColumns);
			} catch (SQLException e) {
				throw new DatabaseOperationException("Error while DB operation", e);
			}
		} else if (tableModel != null) {
			tableModel.list(options);
		} else {
			throw new IllegalStateException("ApiModel was not initialized properly");
		}
	}
}
