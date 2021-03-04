package dsn_metaserver.exception;

public class NodeNotFoundException extends Exception {
	
	private static final long serialVersionUID = 1L;
	
	public NodeNotFoundException() {
		super();
	}
	
	public NodeNotFoundException(final String message) {
		super(message);
	}

}
