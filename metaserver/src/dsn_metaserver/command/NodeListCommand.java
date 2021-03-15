package dsn_metaserver.command;

import java.util.List;

import dnl.utils.text.table.TextTable;
import dsn_metaserver.model.Node;
import dsn_metaserver.model.OnlineNode;

public class NodeListCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		final List<Node> nodes = Node.listNodesDatabase();
//		final Set<Long> onlineNodeIds = Node.getOnlineNodes().stream().map(Node::getId).collect(Collectors.toUnmodifiableSet());
		
		if (nodes.isEmpty()) {
			System.out.println("No nodes");
		} else {
			final List<OnlineNode> onlineNodes = OnlineNode.getOnlineNodes();
			
			if (onlineNodes.isEmpty()) {
				System.out.println("No online nodes");
			} else {
				System.out.println("Online nodes:");
				final String[] columns = {"id", "token", "address", "uptime", "download priority", "upload priority", "name", "label"};
				final Object[][] data = new Object[onlineNodes.size()][columns.length];
				
				for (int i = 0; i < onlineNodes.size(); i++) {
					final OnlineNode node = onlineNodes.get(i);
					data[i][0] = node.getId();
					data[i][1] = node.getFullToken();
					data[i][2] = node.getAddress();
					data[i][3] = node.getUptime();
					data[i][4] = node.getDownloadPriority();
					data[i][5] = node.getUploadPriority();
					data[i][6] = node.getName();
					data[i][7] = node.getLabel();
				}
				
				new TextTable(columns, data).printTable();
			}
			
			nodes.removeIf(onlineNodes::contains);
			
			if (!nodes.isEmpty()) {
				System.out.println("Offline nodes:");
				final String[] columns = {"id", "token", "uptime", "download priority", "upload priority", "name"};
				final Object[][] data = new Object[nodes.size()][columns.length];
				
				for (int i = 0; i < nodes.size(); i++) {
					final Node node = nodes.get(i);
					data[i][0] = node.getId();
					data[i][1] = node.getFullToken();
					data[i][2] = node.getUptime();
					data[i][3] = node.getDownloadPriority();
					data[i][4] = node.getUploadPriority();
					data[i][5] = node.getName();
				}
				
				new TextTable(columns, data).printTable();
			} else {
				System.out.println("No nodes are offline.");
			}
		}
	}

}
