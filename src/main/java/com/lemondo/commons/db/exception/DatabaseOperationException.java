package com.lemondo.commons.db.exception;

public class DatabaseOperationException extends Exception {
	private static final long serialVersionUID = -782188698912575675L;

	public DatabaseOperationException() {
		super();
	}

	public DatabaseOperationException(String arg0) {
		super(arg0);
	}

	public DatabaseOperationException(Throwable arg0) {
		super(arg0);
	}

	public DatabaseOperationException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}
