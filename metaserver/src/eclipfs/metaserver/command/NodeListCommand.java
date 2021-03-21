package eclipfs.metaserver.command;

import java.util.ArrayList;
import java.util.List;

import dnl.utils.text.table.TextTable;
import eclipfs.metaserver.Util;
import eclipfs.metaserver.model.Node;
import eclipfs.metaserver.model.OnlineNode;

public class NodeListCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		final List<Node> nodes = new ArrayList<>(Node.listNodesDatabase());

		if (nodes.isEmpty()) {
			System.out.println("No nodes");
		} else {
			final List<OnlineNode> onlineNodes = OnlineNode.getOnlineNodes();

			if (onlineNodes.isEmpty()) {
				System.out.println("No online nodes");
			} else {
				System.out.println("Online nodes:");
				final String[] columns = {"id", "location", "name", "address", "free space", "token"};
				final Object[][] data = new Object[onlineNodes.size()][columns.length];

				for (int i = 0; i < onlineNodes.size(); i++) {
					final OnlineNode node = onlineNodes.get(i);
					data[i][0] = node.getId();
					data[i][1] = node.getLocation();
					data[i][2] = node.getName();
					data[i][3] = node.getAddress();
					data[i][4] = Util.formatByteCount(node.getFreeSpace());
					data[i][5] = node.getFullToken();
				}

				new TextTable(columns, data).printTable();
			}

			nodes.removeIf(onlineNodes::contains);

			if (!nodes.isEmpty()) {
				System.out.println("Offline nodes:");
				final String[] columns = {"id", "location", "name", "token"};
				final Object[][] data = new Object[nodes.size()][columns.length];

				for (int i = 0; i < nodes.size(); i++) {
					final Node node = nodes.get(i);
					data[i][0] = node.getId();
					data[i][1] = node.getLocation();
					data[i][2] = node.getName();
					data[i][3] = node.getFullToken();
				}

				new TextTable(columns, data).printTable();
			} else {
				System.out.println("No nodes are offline.");
			}
		}
	}

}
