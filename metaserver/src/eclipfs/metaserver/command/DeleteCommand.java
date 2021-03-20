package eclipfs.metaserver.command;

import java.sql.SQLException;

import eclipfs.metaserver.MetaServer;
import eclipfs.metaserver.exception.AlreadyExistsException;
import eclipfs.metaserver.model.Inode;

public class DeleteCommand extends Command {

	@Override
	public void run(final String[] args) throws SQLException, AlreadyExistsException {
		if (args.length != 1) {
			System.out.println("del <name> | <id>");
			return;
		}

		Inode inode;
		try {
			inode = Inode.byId(Long.parseLong(args[0])).orElseThrow(() -> new IllegalArgumentException("Invalid inode (file/directory id)"));
		} catch (final NumberFormatException e) {
			inode = MetaServer.WORKING_DIRECTORY.getChild(args[0]).orElseThrow(() -> new IllegalArgumentException("Invalid file/directory name"));
		}
		inode.delete();
	}

}
