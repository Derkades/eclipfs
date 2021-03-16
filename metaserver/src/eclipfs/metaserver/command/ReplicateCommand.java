package eclipfs.metaserver.command;

import eclipfs.metaserver.Replication;

public class ReplicateCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		if (args.length == 1 && args[0].equals("full")) {
			Replication.addAllChunksToQueue();
			System.out.println("Added all chunks to check queue");
		} else if (args.length == 1) {
			Replication.addRandomChunksToQueue(Integer.parseInt(args[0]));
			System.out.println("Added " + args[0] + " random chunks to check queue");
		} else {
			System.out.println("Usage: replicate full|<chunks amount>");
		}
	}

}
