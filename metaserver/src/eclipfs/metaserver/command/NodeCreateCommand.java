package eclipfs.metaserver.command;

import eclipfs.metaserver.model.Node;

public class NodeCreateCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: nodecreate <name> <location>");
			return;
		}

		final String name = args[0];
		final String location = args[1];
		final Node node = Node.createNode(name, location);

		System.out.println("Created new node '" + node.getFullToken() + "'");
	}

}
