package eclipfs.metaserver.http.endpoints.dashboard;

import java.io.PrintWriter;

public class Dashboard {

//	private static final long serialVersionUID = 1L;
//
//	@Override
//	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
//		final long start = System.currentTimeMillis();
//		try {
//			response.setContentType("text/html");
//			final PrintWriter writer = response.getWriter();
//			writer.print("<!DOCTYPE html>");
//			writer.print("<head>");
//			writer.print("<meta charset=\"utf-8\">");
//			writer.print("<title>eclipfs dashboard</title>");
//			writer.print("<link rel=\"stylesheet\" href=\"/bootstrap.min.css\">");
//			writer.print("<meta http-equiv=\"refresh\" content=\"10;url=.\">");
//			writer.print("</head>");
//			writer.print("<body>");
//			writer.print("<div class=\"container\">");
//			writer.print("<h1>EclipFS</h1>");
//			writer.print("<h2>Nodes</h2>");
//			writer.print("<div id=\"content-nodes\"></div>");
//			{
//				final long start2 = System.currentTimeMillis();
//				writer.print("<h2>Replication</h2>");
//				final String[] columns = {"queue size", "status"};
//				final Object[][] data = new Object[1][columns.length];
//				data[0][0] = Replication.getQueueSize();
//				data[0][1] = Replication.getStatus();
//				writeTable(writer, columns, data);
//				System.out.println(System.currentTimeMillis() - start2);
//			}
//			{
//				final long start2 = System.currentTimeMillis();
//				writer.print("<h2>Filesystem statistics</h2>");
//				final String[] columns = {"files", "directories"};
//				final Object[][] data = new Object[1][columns.length];
//				data[0][0] = Inode.fileCount();
//				data[0][1] = Inode.directoryCount();
//				writeTable(writer, columns, data);
//				System.out.println(System.currentTimeMillis() - start2);
//			}
//			{
//				final long start2 = System.currentTimeMillis();
//				writer.print("<h2>Users</h2>");
//				final String[] columns = {"id", "username", "access"};
//				final List<User> users = User.list();
//				final Object[][] data = new Object[users.size()][columns.length];
//				int row = 0;
//				for (final User user : users) {
//					data[row][0] = user.getId();
//					data[row][1] = user.getUsername();
//					data[row][2] = user.hasWriteAccess() ? "full" : "read-only";
//					row++;
//				}
//
//				writeTable(writer, columns, data);
//				System.out.println(System.currentTimeMillis() - start2);
//			}
//			final ConnectionStatistics stats = MetaServer.getHttpServer().getStatistics();
//			writer.print("<span class=\"text-muted\">");
//			writer.print("Dashboard render took " + (System.currentTimeMillis() - start) + " ms. ");
//			writer.print("Connections: " + stats.getConnections() + " / " + stats.getConnectionsTotal() + ". ");
//			writer.print("Metaserver traffic: " + StringFormatUtils.formatByteCount(stats.getSentBytes()) + " sent / " + StringFormatUtils.formatByteCount(stats.getReceivedBytes()) + " received.");
//			writer.print("</span>");
//			writer.print("</div>");
//			writer.print("<script src=\"/script.js\"></script>");
//			writer.print("</body>");
//			writer.print("</html>");
//		} catch (final SQLException e) {
//			throw new IOException(e);
//		}
//	}

	static void writeTable(final PrintWriter writer, final String[] columns, final Object[][] data) {
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
