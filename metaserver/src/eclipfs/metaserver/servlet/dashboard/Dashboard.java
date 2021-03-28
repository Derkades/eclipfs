package eclipfs.metaserver.servlet.dashboard;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import org.eclipse.jetty.io.ConnectionStatistics;

import eclipfs.metaserver.MetaServer;
import eclipfs.metaserver.Replication;
import eclipfs.metaserver.model.Inode;
import eclipfs.metaserver.model.Node;
import eclipfs.metaserver.model.OnlineNode;
import eclipfs.metaserver.model.User;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import xyz.derkades.derkutils.StringFormatUtils;

public class Dashboard extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		final long start = System.currentTimeMillis();
		try {
			response.setContentType("text/html");
			final PrintWriter writer = response.getWriter();
			writer.print("<!DOCTYPE html>");
			writer.print("<head>");
			writer.print("<meta charset=\"utf-8\">");
			writer.print("<title>eclipfs dashboard</title>");
			writer.print("<link rel=\"stylesheet\" href=\"/bootstrap.min.css\">");
			writer.print("<meta http-equiv=\"refresh\" content=\"10;url=.\">");
			writer.print("</head>");
			writer.print("<body>");
			writer.print("<div class=\"container\">");
			writer.print("<h1>EclipFS</h1>");
			{
				writer.print("<h2>Nodes</h2>");
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
					data[row][7] = StringFormatUtils.formatByteCount(node.getStoredChunkSize());
					row++;
				}
				writeTable(writer, columns, data);
			}
			{
				writer.print("<h2>Replication</h2>");
				final String[] columns = {"queue size", "status"};
				final Object[][] data = new Object[1][columns.length];
				data[0][0] = Replication.getQueueSize();
				data[0][1] = Replication.getStatus();
				writeTable(writer, columns, data);
			}
			{
				writer.print("<h2>Filesystem statistics</h2>");
				final String[] columns = {"files", "directories"};
				final Object[][] data = new Object[1][columns.length];
				data[0][0] = Inode.fileCount();
				data[0][1] = Inode.directoryCount();
				writeTable(writer, columns, data);
			}
			{
				writer.print("<h2>Users</h2>");
				final String[] columns = {"id", "username", "access"};
				final List<User> users = User.list();
				final Object[][] data = new Object[users.size()][columns.length];
				int row = 0;
				for (final User user : users) {
					data[row][0] = user.getId();
					data[row][1] = user.getUsername();
					data[row][2] = user.hasWriteAccess() ? "full" : "read-only";
					row++;
				}

				writeTable(writer, columns, data);
			}
			final ConnectionStatistics stats = MetaServer.getHttpServer().getStatistics();
			writer.print("<span class=\"text-muted\">");
			writer.print("Dashboard render took " + (System.currentTimeMillis() - start) + " ms. ");
			writer.print("Connections: " + stats.getConnections() + " / " + stats.getConnectionsTotal() + ". ");
			writer.print("Traffic: " + StringFormatUtils.formatByteCount(stats.getSentBytes()) + " sent / " + StringFormatUtils.formatByteCount(stats.getReceivedBytes()) + " received.");
			writer.print("</span>");
			writer.print("</div>");
			writer.print("</body>");
			writer.print("</html>");
		} catch (final SQLException e) {
			throw new IOException(e);
		}
	}

	public void writeTable(final PrintWriter writer, final String[] columns, final Object[][] data) {
		writer.print("<table class=\"table\">");
		writer.print("<thead>");
		writer.print("<tr>");
		for (final String col : columns) {
			writer.print("<th scope=\"col\">" + col + "</th>");
		}
		writer.print("</tr>");
		writer.print("</thead>");
		writer.print("<tbody>");
		for (final Object[] row : data) {
			writer.print("<tr>");
			for (final Object col : row) {
				writer.print("<td>" + col + "</td>");
			}
			writer.print("</tr>");
		}
		writer.print("</tbody>");
		writer.print("</table>");
	}

}
