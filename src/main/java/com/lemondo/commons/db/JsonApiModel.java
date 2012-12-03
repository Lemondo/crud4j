package com.lemondo.commons.db;

import org.json.JSONArray;
import org.json.JSONObject;

import com.lemondo.commons.db.meta.TableMetaData;
import com.lemondo.commons.db.processor.JsonDataProcessor;

public class JsonApiModel extends ApiModel<JSONObject, JSONArray> {

	public JsonApiModel(Helper helper) {
		super(helper, new JsonDataProcessor());
	}

	public JsonApiModel(Helper helper, TableMetaData meta) {
		super(helper, new JsonDataProcessor(), meta);
	}

}
