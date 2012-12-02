package com.lemondo.commons.db;

import java.io.OutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

	private PreparedStatement prepareInsertStmnt(Object key, Map<String, Object> body) {
		Set<String> columns = body.keySet();
		try {
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
					throw new RuntimeException("BOOM: invalid field!");
				}
			}

			return stmnt;
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

	private PreparedStatement prepareInsertStmnt(Map<String, Object> body) {
		return prepareInsertStmnt(null, body);
	}

	private PreparedStatement prepareUpdateStmnt(Object key, Map<String, Object> body) {
		Set<String> columns = body.keySet();
		try {
			PreparedStatement stmnt = helper.prepareStatement(metaData.genUpdateSql(columns));

			int i = 1;
			for (String col : columns) {
				if (columnDef.containsKey(col)) {
					stmnt.setObject(i++, body.get(col), columnDef.get(col));
				} else {
					throw new RuntimeException("BOOM: invalid field!");
				}
			}
			stmnt.setObject(i, key, metaData.getPkType().sqlType);

			return stmnt;
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

	private PreparedStatement prepareDeleteStmnt(Object key) {
		try {
			PreparedStatement stmnt = helper.prepareStatement(metaData.genDeleteSql());

			stmnt.setObject(1, key, metaData.getPkType().sqlType);

			return stmnt;
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

	private Set<FilterCondition> extractFilterFields(Map<String, Object> options) {
		Object filterRaw = options.get("filter");
		if (filterRaw == null) {
			return null;
		} else if (!(filterRaw instanceof Map)) {
			throw new RuntimeException("BOOM!");
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
			throw new RuntimeException("BOOM!");
		} else {
			@SuppressWarnings("unchecked")
			List<String> sortFields = (List<String>) sortFieldsRaw;
			return sortFields;
		}
	}

	private PreparedStatement prepareSelectStmnt(Object key, Map<String, Object> options) {
		Set<FilterCondition> filter = null;
		List<String> sortFields = null;
		if (options != null) {
			filter = extractFilterFields(options);
			sortFields = extractSortFields(options);
		}

		boolean allRows = key == null;

		try {
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
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

	@Override
	public void create(Object key, T body) {
		try {
			prepareInsertStmnt(key, processor.bodyAsMap(body)).executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

	@Override
	public Object create(T body) {
		Object generatedKey = null;
		try {
			PreparedStatement stmnt = prepareInsertStmnt(processor.bodyAsMap(body));
			stmnt.executeUpdate();
			ResultSet rs = stmnt.getGeneratedKeys();
			if (rs.next()) {
				PrimarykeyType pkType = metaData.getPkType();
				switch (pkType){
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
			throw new RuntimeException("BOOM!", e);
		}
		return generatedKey;
	}

	@Override
	public int update(Object key, T body) {
		try {
			return prepareUpdateStmnt(key, processor.bodyAsMap(body)).executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

	@Override
	public int delete(Object key) {
		try {
			return prepareDeleteStmnt(key).executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

	@Override
	public T read(Object key) {
		try {
			ResultSet rs = prepareSelectStmnt(key, null).executeQuery();

			if (rs.next()) {
				ResultSetMetaData rsmd = rs.getMetaData();
				return processor.readRow(rs, rsmd, rsmd.getColumnCount());
			} else {
				throw new RuntimeException("BOOM: No data found");
			}
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

	@Override
	public L list(Map<String, Object> options) {
		try {
			ResultSet rs = prepareSelectStmnt(null, options).executeQuery();

			ResultSetMetaData rsmd = rs.getMetaData();
			int numColumns = rsmd.getColumnCount();

			return processor.readAll(rs, rsmd, numColumns);
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

	@Override
	public void list(OutputStream out, Map<String, Object> options) {
		try {
			ResultSet rs = prepareSelectStmnt(null, options).executeQuery();

			ResultSetMetaData rsmd = rs.getMetaData();
			int numColumns = rsmd.getColumnCount();

			processor.writeRows(out, rs, rsmd, numColumns);
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

}
