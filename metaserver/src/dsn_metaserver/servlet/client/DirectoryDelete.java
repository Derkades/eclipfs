package dsn_metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import com.google.gson.JsonObject;

import dsn_metaserver.model.Directory;
import dsn_metaserver.model.User;
import dsn_metaserver.servlet.ApiError;
import dsn_metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class DirectoryDelete extends HttpServlet {
	
	private static final long serialVersionUID = 1L;

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
			final Optional<User> optUser = ClientAuthentication.verify(request, response);
			
			if (optUser.isEmpty()) {
				return;
			}
			
			final User user = optUser.get();
			
			if (!user.hasWriteAccess()) {
				ApiError.MISSING_WRITE_ACCESS.send(response);
				return;
			}
			
			final JsonObject json = HttpUtil.readJsonFromRequestBody(request, response);
			
			if (json == null) {
				return;
			}
			
			final Directory directory = HttpUtil.getJsonDirectory(json, response);
			if (directory == null) {
				return;
			}
			
			if (directory.isEmpty()) {
				directory.delete();
			} else {
				ApiError.DIRECTORY_NOT_EMPTY.send(response);
				return;
			}
			
			HttpUtil.writeSuccessTrueJson(response);
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
			return;
		}
	}

}
