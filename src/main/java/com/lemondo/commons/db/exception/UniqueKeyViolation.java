package com.lemondo.commons.db.exception;

public class UniqueKeyViolation extends DatabaseOperationException {
	private static final long serialVersionUID = 1917036913557968551L;

	public UniqueKeyViolation() {
		super();
	}

	public UniqueKeyViolation(String arg0) {
		super(arg0);
	}

	public UniqueKeyViolation(Throwable arg0) {
		super(arg0);
	}

	public UniqueKeyViolation(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}
