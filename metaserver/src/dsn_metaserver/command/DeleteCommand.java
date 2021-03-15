package dsn_metaserver.command;

import java.sql.SQLException;

import dsn_metaserver.MetaServer;
import dsn_metaserver.exception.AlreadyExistsException;
import dsn_metaserver.model.Inode;

public class DeleteCommand extends Command {

	@Override
	public void run(final String[] args) throws SQLException, AlreadyExistsException {
		if (args.length != 1) {
			System.out.println("<name>");
			return;
		}
		
		final Inode inode = MetaServer.WORKING_DIRECTORY.getChild(args[0]).get();
		inode.delete();
	}

}
