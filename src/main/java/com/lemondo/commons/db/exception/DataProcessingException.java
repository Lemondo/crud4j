package com.lemondo.commons.db.exception;

public class DataProcessingException extends Exception {
	private static final long serialVersionUID = -1046065117851204215L;

	public DataProcessingException() {
		super();
	}

	public DataProcessingException(String arg0) {
		super(arg0);
	}

	public DataProcessingException(Throwable arg0) {
		super(arg0);
	}

	public DataProcessingException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}
