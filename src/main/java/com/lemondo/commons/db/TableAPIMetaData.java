package com.lemondo.commons.db;

import java.util.List;
import java.util.Set;

public class TableAPIMetaData implements ModelMetaData {

	private TableMetaData tableMetaData;

	private ProcMetaData insertApi;
	private ProcMetaData updateApi;
	private ProcMetaData deleteApi;
	private ProcMetaData readApi;
	private ProcMetaData listApi;

	public TableAPIMetaData() {
		this.tableMetaData = null;
	}

	public TableAPIMetaData(TableMetaData tableMetaData) {
		this.tableMetaData = tableMetaData;
	}

	public void setTableMetaData(TableMetaData tableMetaData) {
		this.tableMetaData = tableMetaData;
	}
	
	public ProcMetaData getInsertApi() {
		return insertApi;
	}

	public void setInsertApi(ProcMetaData insertApi) {
		this.insertApi = insertApi;
	}

	public ProcMetaData getUpdateApi() {
		return updateApi;
	}

	public void setUpdateApi(ProcMetaData updateApi) {
		this.updateApi = updateApi;
	}

	public ProcMetaData getDeleteApi() {
		return deleteApi;
	}

	public void setDeleteApi(ProcMetaData deleteApi) {
		this.deleteApi = deleteApi;
	}

	public ProcMetaData getReadApi() {
		return readApi;
	}

	public void setReadApi(ProcMetaData readApi) {
		this.readApi = readApi;
	}

	public ProcMetaData getListApi() {
		return listApi;
	}

	public void setListApi(ProcMetaData listApi) {
		this.listApi = listApi;
	}

	@Override
	public String genInsertSql(Set<String> columns) {
		if (insertApi != null) {
			return insertApi.genProcedureCall();
		} else if (tableMetaData != null) {
			return tableMetaData.genInsertSql(columns);
		} else {
			throw new RuntimeException("BOOM!");
		}
	}

	@Override
	public String genUpdateSql(Set<String> columns) {
		if (updateApi != null) {
			return updateApi.genProcedureCall();
		} else if (tableMetaData != null) {
			return tableMetaData.genUpdateSql(columns);
		} else {
			throw new RuntimeException("BOOM!");
		}
	}

	@Override
	public String genDeleteSql() {
		if (deleteApi != null) {
			return deleteApi.genProcedureCall();
		} else if (tableMetaData != null) {
			return tableMetaData.genDeleteSql();
		} else {
			throw new RuntimeException("BOOM!");
		}
	}

	@Override
	public String genSelectSql(boolean allRows, Set<FilterCondition> filter, List<String> sortFields) {
		ProcMetaData selectApi = allRows ? listApi : readApi;
		if (selectApi != null) {
			return selectApi.genProcedureCall();
		} else if (tableMetaData != null) {
			return tableMetaData.genSelectSql(allRows, filter, sortFields);
		} else {
			throw new RuntimeException("BOOM!");
		}
	}

}
