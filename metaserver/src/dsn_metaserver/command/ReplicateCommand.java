package dsn_metaserver.command;

import dsn_metaserver.Replication;

public class ReplicateCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		Replication.start();
	}

}
