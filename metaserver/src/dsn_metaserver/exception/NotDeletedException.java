package dsn_metaserver.exception;

public class NotDeletedException extends Exception {
	
	private static final long serialVersionUID = 1L;
	
	public NotDeletedException() {
		super();
	}
	
	public NotDeletedException(final String message) {
		super(message);
	}

}
