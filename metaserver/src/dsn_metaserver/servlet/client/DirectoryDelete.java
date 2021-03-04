package dsn_metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import com.google.gson.JsonObject;

import dsn_metaserver.exception.NotEmptyException;
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
			
			final JsonObject json = HttpUtil.readJsonFromRequestBody(request, response);
			
			if (json == null) {
				return;
			}
			
			final String path = HttpUtil.getJsonString(json, "path", response);
			if (path == null) {
				return;
			}
			
			final Optional<Directory> optDir = Directory.findByPath(path);
			
			if (optDir.isEmpty()) {
				ApiError.DIRECTORY_NOT_EXISTS.send(response);
				return;
			}
			
			final Directory dir = optDir.get();
			
			if (!user.hasWriteAccess()) {
				ApiError.MISSING_WRITE_ACCESS.send(response);
				return;
			}
			
			try {
				dir.delete();
				response.getWriter().write("{\"success\": true}");
				response.getWriter().flush();
			} catch (final NotEmptyException e) {
				ApiError.DIRECTORY_NOT_EMPTY.send(response);
				return;
			}
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
			return;
		}
	}

}
