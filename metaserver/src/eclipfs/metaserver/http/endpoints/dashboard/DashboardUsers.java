package eclipfs.metaserver.http.endpoints.dashboard;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import eclipfs.metaserver.http.HttpUtil;
import eclipfs.metaserver.model.User;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class DashboardUsers extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		response.setContentType("text/html");
		try {
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
			Dashboard.writeTable(response.getWriter(), columns, data);
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}

}
