package eclipfs.metaserver.command;

import java.sql.SQLException;

import eclipfs.metaserver.MetaServer;
import eclipfs.metaserver.exception.AlreadyExistsException;
import eclipfs.metaserver.model.Inode;

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
