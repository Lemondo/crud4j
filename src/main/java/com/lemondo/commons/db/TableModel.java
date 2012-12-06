package com.lemondo.commons.db;

import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.lemondo.commons.db.exception.DataProcessingException;
import com.lemondo.commons.db.exception.DatabaseOperationException;
import com.lemondo.commons.db.exception.ForeignKeyViolation;
import com.lemondo.commons.db.exception.InvalidFieldException;
import com.lemondo.commons.db.exception.NoDataFoundException;
import com.lemondo.commons.db.exception.NotNullViolation;
import com.lemondo.commons.db.exception.UniqueKeyViolation;
import com.lemondo.commons.db.meta.FilterCondition;
import com.lemondo.commons.db.meta.TableMetaData;
import com.lemondo.commons.db.processor.DataProcessor;

public class TableModel<T, L> implements Model<T, L> {

	private final TableMetaData metaData;
	private final Map<String, Integer> columnDef;

	private final Helper helper;

	private final DataProcessor<T, L> processor;

	public TableModel(TableMetaData meta, Helper helper, DataProcessor<T, L> processor) {
		this.metaData = meta;
		this.columnDef = (this.metaData == null) ? null : this.metaData.getColumnDef();
		this.helper = helper;
		this.processor = processor;
	}

	private PreparedStatement prepareInsertStmnt(Object key, Map<String, Object> body) throws InvalidFieldException, SQLException {
		Set<String> columns = body.keySet();
		int autoGenKeys = (key == null) ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS;
		PreparedStatement stmnt = helper.prepareStatement(metaData.genInsertSql(columns, (key == null)), autoGenKeys);

		int i = 1;

		if (key != null) {
			stmnt.setObject(i++, key, metaData.getPkType().sqlType);
		}

		for (String col : columns) {
			if (columnDef.containsKey(col)) {
				stmnt.setObject(i++, body.get(col), columnDef.get(col));
			} else {
				throw new InvalidFieldException("Invalid field, " + col);
			}
		}

		return stmnt;
	}

	private PreparedStatement prepareInsertStmnt(Map<String, Object> body) throws InvalidFieldException, SQLException {
		return prepareInsertStmnt(null, body);
	}

	private PreparedStatement prepareUpdateStmnt(Object key, Map<String, Object> body) throws InvalidFieldException, SQLException {
		Set<String> columns = body.keySet();

		PreparedStatement stmnt = helper.prepareStatement(metaData.genUpdateSql(columns));

		int i = 1;
		for (String col : columns) {
			if (columnDef.containsKey(col)) {
				stmnt.setObject(i++, body.get(col), columnDef.get(col));
			} else {
				throw new InvalidFieldException("Invalid field, " + col);
			}
		}
		stmnt.setObject(i, key, metaData.getPkType().sqlType);

		return stmnt;
	}

	private PreparedStatement prepareDeleteStmnt(Object key) throws SQLException {
		PreparedStatement stmnt = helper.prepareStatement(metaData.genDeleteSql());

		stmnt.setObject(1, key, metaData.getPkType().sqlType);

		return stmnt;
	}

	private Set<FilterCondition> extractFilterFields(Map<String, Object> options) {
		Object filterRaw = options.get("filter");
		if (filterRaw == null) {
			return null;
		} else if (!(filterRaw instanceof Map)) {
			throw new IllegalArgumentException("\"filter\" attribute must be an instance of Map");
		} else {
			Set<FilterCondition> filter = new HashSet<FilterCondition>();

			@SuppressWarnings("unchecked")
			Map<String, Object> filterMap = (Map<String, Object>) filterRaw;

			Set<String> filterFields = filterMap.keySet();
			for (String field : filterFields) {
				if (columnDef.containsKey(field)) {
					filter.add(new FilterCondition(field, "=", filterMap.get(field), columnDef.get(field)));
				}
			}

			return filter;
		}
	}

	private List<String> extractSortFields(Map<String, Object> options) {
		Object sortFieldsRaw = options.get("order");
		if (sortFieldsRaw == null) {
			return null;
		} else if (!(sortFieldsRaw instanceof List)) {
			throw new IllegalArgumentException("\"order\" attribute must be an instance of List");
		} else {
			@SuppressWarnings("unchecked")
			List<String> sortFields = (List<String>) sortFieldsRaw;
			return sortFields;
		}
	}

	private PreparedStatement prepareSelectStmnt(Object key, Map<String, Object> options) throws SQLException {
		Set<FilterCondition> filter = null;
		List<String> sortFields = null;
		if (options != null) {
			filter = extractFilterFields(options);
			sortFields = extractSortFields(options);
		}

		boolean allRows = key == null;

		PreparedStatement stmnt = helper.prepareStatement(metaData.genSelectSql(allRows, filter, sortFields));

		int i = 1;
		if (!allRows) {
			stmnt.setObject(i++, key, metaData.getPkType().sqlType);
		}

		if (filter != null) {
			for (FilterCondition condition : filter) {
				stmnt.setObject(i++, condition.getValue(), condition.getType());
			}
		}
		// TODO Add paging here

		return stmnt;
	}

	@Override
	public void create(Object key, T body) throws InvalidFieldException, DataProcessingException, DatabaseOperationException {
		try {
			prepareInsertStmnt(key, processor.bodyAsMap(body)).executeUpdate();
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
	}

	@Override
	public Object create(T body) throws InvalidFieldException, DataProcessingException, DatabaseOperationException {
		Object generatedKey = null;
		try {
			PreparedStatement stmnt = prepareInsertStmnt(processor.bodyAsMap(body));
			stmnt.executeUpdate();
			ResultSet rs = stmnt.getGeneratedKeys();
			if (rs.next()) {
				PrimarykeyType pkType = metaData.getPkType();
				switch (pkType) {
				case VARCHAR:
					generatedKey = rs.getString(1);
					break;
				case INTEGER:
					generatedKey = rs.getInt(1);
					break;
				case LONG:
					generatedKey = rs.getLong(1);
					break;
				}
			}
		} catch (SQLException e) {
			if (Helper.isNotNullViolation(e)) {
				throw new NotNullViolation("Mandatory field is omitted", e);
			} else if (Helper.isForeignKeyViolation(e)) {
				throw new ForeignKeyViolation("Parent record not found", e);
			} else {
				throw new DatabaseOperationException("Error while DB operation", e);
			}
		}
		return generatedKey;
	}

	@Override
	public int update(Object key, T body) throws InvalidFieldException, DataProcessingException, DatabaseOperationException {
		try {
			return prepareUpdateStmnt(key, processor.bodyAsMap(body)).executeUpdate();
		} catch (SQLException e) {
			if (Helper.isNotNullViolation(e)) {
				throw new NotNullViolation("Mandatory field is omitted", e);
			} else if (Helper.isForeignKeyViolation(e)) {
				throw new ForeignKeyViolation("Foreign key violation", e);
			} else {
				throw new DatabaseOperationException("Error while DB operation", e);
			}
		}
	}

	@Override
	public int delete(Object key) throws DatabaseOperationException {
		try {
			return prepareDeleteStmnt(key).executeUpdate();
		} catch (SQLException e) {
			if (Helper.isForeignKeyViolation(e)) {
				throw new ForeignKeyViolation("Cannot delete, child record found", e);
			} else {
				throw new DatabaseOperationException("Error while DB operation", e);
			}
		}
	}

	@Override
	public T read(Object key) throws NoDataFoundException, DataProcessingException, DatabaseOperationException {
		try {
			ResultSet rs = prepareSelectStmnt(key, null).executeQuery();

			if (rs.next()) {
				ResultSetMetaData rsmd = rs.getMetaData();
				return processor.readRow(rs, rsmd, rsmd.getColumnCount());
			} else {
				throw new NoDataFoundException("Could not find record with given key");
			}
		} catch (SQLException e) {
			throw new DatabaseOperationException("Error while DB operation", e);
		}
	}

	@Override
	public L list(Map<String, Object> options) throws DataProcessingException, DatabaseOperationException {
		try {
			ResultSet rs = prepareSelectStmnt(null, options).executeQuery();

			ResultSetMetaData rsmd = rs.getMetaData();
			int numColumns = rsmd.getColumnCount();

			return processor.readAll(rs, rsmd, numColumns);
		} catch (SQLException e) {
			throw new DatabaseOperationException("Error while DB operation", e);
		}
	}

	@Override
	public void list(OutputStream out, Map<String, Object> options) throws DataProcessingException, DatabaseOperationException {
		try {
			list(out, options, null);
		} catch (UnsupportedEncodingException e) {
		}
	}

	@Override
	public void list(OutputStream out, Map<String, Object> options, String encoding) throws DataProcessingException, DatabaseOperationException,
			UnsupportedEncodingException {
		try {
			ResultSet rs = prepareSelectStmnt(null, options).executeQuery();

			ResultSetMetaData rsmd = rs.getMetaData();
			int numColumns = rsmd.getColumnCount();

			if (encoding != null) {
				processor.writeRows(out, rs, rsmd, numColumns, encoding);
			} else {
				processor.writeRows(out, rs, rsmd, numColumns);
			}
		} catch (SQLException e) {
			throw new DatabaseOperationException("Error while DB operation", e);
		}
	}

}
