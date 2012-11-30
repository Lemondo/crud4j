package com.lemondo.commons.db;

import org.json.JSONArray;
import org.json.JSONObject;

public class JsonModel extends TableModel<JSONObject, JSONArray> {

	public JsonModel(TableMetaData meta, Helper helper) {
		super(meta, helper, new JsonDataProcessor());
	}

}
