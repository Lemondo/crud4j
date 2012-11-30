package com.lemondo.commons.db;

import org.json.JSONArray;
import org.json.JSONObject;

import com.lemondo.commons.db.meta.TableMetaData;
import com.lemondo.commons.db.processor.JsonDataProcessor;

public class JsonTableModel extends TableModel<JSONObject, JSONArray> {

	public JsonTableModel(TableMetaData meta, Helper helper) {
		super(meta, helper, new JsonDataProcessor());
	}

}
