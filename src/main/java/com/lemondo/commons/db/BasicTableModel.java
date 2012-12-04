package com.lemondo.commons.db;

import java.util.List;
import java.util.Map;

import com.lemondo.commons.db.meta.TableMetaData;
import com.lemondo.commons.db.processor.BasicDataProcessor;

public class BasicTableModel extends TableModel<Map<String, Object>, List<Map<String, Object>>> {

	public BasicTableModel(TableMetaData meta, Helper helper) {
		super(meta, helper, new BasicDataProcessor());
	}

}
