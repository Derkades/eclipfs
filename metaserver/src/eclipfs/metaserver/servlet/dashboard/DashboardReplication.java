package eclipfs.metaserver.servlet.dashboard;

import java.io.IOException;

import eclipfs.metaserver.Replication;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class DashboardReplication extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		response.setContentType("text/html");
		final String[] columns = {"queue size", "status"};
		final Object[][] data = new Object[1][columns.length];
		data[0][0] = Replication.getQueueSize();
		data[0][1] = Replication.getStatus();
		Dashboard.writeTable(response.getWriter(), columns, data);
	}

}
