package dsn_metaserver.exception;

public class NotEmptyException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public NotEmptyException(final String message) {
		super(message);
	}

}
