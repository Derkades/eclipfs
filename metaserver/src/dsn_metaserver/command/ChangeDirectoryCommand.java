package dsn_metaserver.command;

import java.sql.SQLException;
import java.util.Optional;

import dsn_metaserver.MetaServer;
import dsn_metaserver.exception.NotADirectoryException;
import dsn_metaserver.exception.NotExistsException;
import dsn_metaserver.model.Directory;
import dsn_metaserver.model.File;
import dsn_metaserver.model.Inode;

public class ChangeDirectoryCommand extends Command {

	@Override
	public void run(final String[] args) throws SQLException, NotExistsException, NotADirectoryException {
		if (args.length != 1) {
			System.out.println("<path>");
		}
		
		String path = args[0];
		
		if (!path.startsWith("/")) {
			// Relative path
			path = MetaServer.WORKING_DIRECTORY.getAbsolutePath() + "/" + path;
		}
		
		final Optional<Inode> opt = Inode.findByPath(path);
		
		if (opt.isEmpty()) {
			System.out.print("This directory does not exist");
			return;
		}
		
		final Inode inode = opt.get();
		
		if (inode instanceof File) {
			System.out.println("The provided name is a file, not a directory");
			return;
		}
		
		MetaServer.WORKING_DIRECTORY = (Directory) inode;
	}

}
