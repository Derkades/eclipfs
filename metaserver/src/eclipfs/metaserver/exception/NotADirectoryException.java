package eclipfs.metaserver.exception;

import java.sql.SQLException;

import eclipfs.metaserver.model.Inode;

public class NotADirectoryException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public NotADirectoryException(final Inode inode) throws SQLException {
		super(inode.getAbsolutePath());
	}

}
