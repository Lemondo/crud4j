package com.lemondo.commons.db.exception;

public class InvalidFieldException extends Exception {
	private static final long serialVersionUID = 3505519047540008782L;

	public InvalidFieldException() {
		super();
	}

	public InvalidFieldException(String arg0) {
		super(arg0);
	}

	public InvalidFieldException(Throwable arg0) {
		super(arg0);
	}

	public InvalidFieldException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}
