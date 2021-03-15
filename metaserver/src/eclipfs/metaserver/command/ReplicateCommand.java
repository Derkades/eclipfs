package eclipfs.metaserver.command;

import eclipfs.metaserver.Replication;

public class ReplicateCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		Replication.start();
	}

}
