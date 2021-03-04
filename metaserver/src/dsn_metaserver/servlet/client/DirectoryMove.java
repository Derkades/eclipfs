package dsn_metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import com.google.gson.JsonObject;

import dsn_metaserver.Validation;
import dsn_metaserver.exception.AlreadyExistsException;
import dsn_metaserver.exception.NotExistsException;
import dsn_metaserver.model.Directory;
import dsn_metaserver.model.User;
import dsn_metaserver.servlet.ApiError;
import dsn_metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class DirectoryMove extends HttpServlet {
	
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
			
			final String oldPath = HttpUtil.getJsonString(json, "path_old", response);
			final String newPath = HttpUtil.getJsonString(json, "path_new", response);
			if (oldPath == null || newPath == null) {
				return;
			}
			
			Validation.validatePath(oldPath);
			Validation.validatePath(newPath);
			
			if (oldPath.equals(newPath)) {
				throw new IllegalArgumentException();
				// TODO API error
			}
			
			final Optional<Directory> optDir = Directory.findByPath(oldPath);
			
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
				dir.move(newPath);
				response.getWriter().write("{\"success\": true}");
			} catch (final AlreadyExistsException e) {
				ApiError.DIRECTORY_ALREADY_EXISTS.send(response);
			} catch (final NotExistsException e) {
				ApiError.DIRECTORY_NOT_EXISTS.send(response);
			}
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
			return;
		}
	}

}
