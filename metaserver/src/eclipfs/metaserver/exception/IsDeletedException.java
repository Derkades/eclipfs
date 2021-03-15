package eclipfs.metaserver.exception;

public class IsDeletedException extends Exception {
	
	private static final long serialVersionUID = 1L;
	
	public IsDeletedException() {
		super();
	}
	
	public IsDeletedException(final String message) {
		super(message);
	}

}
