package eclipfs.metaserver.command;

import eclipfs.metaserver.MetaServer;

public class UpCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		if (MetaServer.WORKING_DIRECTORY.isRootDirectory()) {
			System.out.println("Cannot go to parent directory, we're already in the root directory");
		} else {
			MetaServer.WORKING_DIRECTORY = MetaServer.WORKING_DIRECTORY.getParent();
		}
	}

}
