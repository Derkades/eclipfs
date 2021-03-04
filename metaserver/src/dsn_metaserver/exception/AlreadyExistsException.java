package dsn_metaserver.exception;

public class AlreadyExistsException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public AlreadyExistsException(final String message) {
		super(message);
	}

}
