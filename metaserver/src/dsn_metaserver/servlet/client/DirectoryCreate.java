package dsn_metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import dsn_metaserver.exception.AlreadyExistsException;
import dsn_metaserver.model.Directory;
import dsn_metaserver.model.User;
import dsn_metaserver.servlet.ApiError;
import dsn_metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class DirectoryCreate extends HttpServlet {

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
			
			final Directory parent = HttpUtil.getJsonDirectory(json, response);
			final String name = HttpUtil.getJsonString(json, response, "name");
			
			if (parent == null || name == null) {
				return;
			}
			
			Directory newDirectory;
			try {
				newDirectory = parent.createDirectory(name);
			} catch (final AlreadyExistsException e) {
				ApiError.DIRECTORY_ALREADY_EXISTS.send(response);
				return;
			}
			
			try (JsonWriter jsonResponse = new JsonWriter(response.getWriter())) {
				jsonResponse.beginObject();
				jsonResponse.name("directory");
				DirectoryInfo.writeDirectoryInfoJson(newDirectory, jsonResponse);
				jsonResponse.endObject();
			}
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}

}
