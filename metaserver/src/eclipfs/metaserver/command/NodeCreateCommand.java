package eclipfs.metaserver.command;

import eclipfs.metaserver.model.Node;

public class NodeCreateCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		final Node node = Node.createNode();
		
		System.out.println("Created new node '" + node.getFullToken() + "'");
	}

}
