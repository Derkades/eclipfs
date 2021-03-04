package dsn_metaserver.exception;

public class NotExistsException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public NotExistsException() {
		super();
	}
	
	public NotExistsException(final String message) {
		super(message);
	}

}
