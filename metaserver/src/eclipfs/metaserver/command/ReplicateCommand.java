package eclipfs.metaserver.command;

import java.sql.SQLException;

import eclipfs.metaserver.Replication;
import eclipfs.metaserver.model.Directory;
import eclipfs.metaserver.model.File;
import eclipfs.metaserver.model.Inode;

public class ReplicateCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		if (args.length == 1 && args[0].equals("full")) {
			System.out.println("Adding all chunks to check queue...");
			queueAll(Inode.getRootInode());
		}
		Replication.start();
	}
	
	private void queueAll(final Directory directory) throws SQLException {
		for (final File file : directory.listFiles()) {
			for (final long id : file.listChunkIds()) {
				Replication.addToCheckQueue(id);
			}
		}
		for (final Directory dir : directory.listDirectories()) {
			queueAll(dir);
		}
	}

}
