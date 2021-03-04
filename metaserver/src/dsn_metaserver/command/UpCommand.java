package dsn_metaserver.command;

import dsn_metaserver.MetaServer;
import dsn_metaserver.model.Directory;

public class UpCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		if (MetaServer.WORKING_DIRECTORY.isEmpty()) {
			System.out.println("Cannot go to parent directory, we're already in the root directory");
		} else {
			final Directory directory = MetaServer.WORKING_DIRECTORY.get();
			MetaServer.WORKING_DIRECTORY = directory.getParent();
		}
	}

}
