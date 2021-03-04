package dsn_metaserver.command;

import dsn_metaserver.model.Node;

public class NodeCreateCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		final Node node = Node.createNode();
		
		System.out.println("Created new node '" + node.getFullToken() + "'");
	}

}
