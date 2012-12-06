package com.lemondo.commons.db;

import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import com.lemondo.commons.db.exception.DataProcessingException;
import com.lemondo.commons.db.exception.DatabaseOperationException;
import com.lemondo.commons.db.exception.InvalidFieldException;
import com.lemondo.commons.db.exception.NoDataFoundException;

public interface Model<T, L> {

	public void create(Object key, T body) throws InvalidFieldException, DataProcessingException, DatabaseOperationException;

	public Object create(T body) throws InvalidFieldException, DataProcessingException, DatabaseOperationException;

	public int update(Object key, T body) throws InvalidFieldException, DataProcessingException, DatabaseOperationException;

	public int delete(Object key) throws DatabaseOperationException;

	public T read(Object key) throws NoDataFoundException, DataProcessingException, DatabaseOperationException;

	public L list(Map<String, Object> options) throws DataProcessingException, DatabaseOperationException;

	public void list(OutputStream out, Map<String, Object> options) throws DataProcessingException, DatabaseOperationException;

	public void list(OutputStream out, Map<String, Object> options, String encoding) throws DataProcessingException, DatabaseOperationException, UnsupportedEncodingException;

}
