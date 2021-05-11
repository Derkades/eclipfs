package eclipfs.metaserver.servlet.dashboard;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import eclipfs.metaserver.model.Node;
import eclipfs.metaserver.model.OnlineNode;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import xyz.derkades.derkutils.StringFormatUtils;

public class DashboardNodes extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
			response.setContentType("text/html");
			final String[] columns = {"id", "location", "name", "online", "address", "free space", "stored chunks", "stored chunks size"};
			final List<Node> nodes = Node.listNodesDatabase();
			final Object[][] data = new Object[nodes.size()][columns.length];
			int row = 0;
			for (final Node node : nodes) {
				data[row][0] = node.getId();
				data[row][1] = node.getLocation();
				data[row][2] = node.getName();
				final Optional<OnlineNode> online = OnlineNode.getOnlineNodeById(node.getId());
				if (online.isPresent()) {
					data[row][3] = "yes";
					data[row][4] = online.get().getAddress();
					data[row][5] = StringFormatUtils.formatByteCount(online.get().getFreeSpace());
				} else {
					data[row][3] = "no";
					data[row][4] = "-";
					data[row][5] = "-";
				}
				data[row][6] = node.getStoredChunkCount();
//					data[row][7] = StringFormatUtils.formatByteCount(node.getStoredChunkSize());
				row++;
			}
			Dashboard.writeTable(response.getWriter(), columns, data);
	} catch (final SQLException e) {
			throw new IOException(e);
		}
	}

}
