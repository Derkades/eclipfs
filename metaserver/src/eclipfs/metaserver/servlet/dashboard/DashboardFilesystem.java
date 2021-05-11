package eclipfs.metaserver.servlet.dashboard;

import java.io.IOException;
import java.sql.SQLException;

import eclipfs.metaserver.model.Inode;
import eclipfs.metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class DashboardFilesystem extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		response.setContentType("text/html");
		try {
			final String[] columns = { "files", "directories" };
			final Object[][] data = new Object[1][columns.length];
			data[0][0] = Inode.fileCount();
			data[0][1] = Inode.directoryCount();
			Dashboard.writeTable(response.getWriter(), columns, data);
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}

}
