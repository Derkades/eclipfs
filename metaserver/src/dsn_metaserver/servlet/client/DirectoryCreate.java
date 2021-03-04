package dsn_metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import dsn_metaserver.Validation;
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
		Optional<User> optUser;
		try {
			optUser = ClientAuthentication.verify(request, response);
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
			return;
		}
		
		if (optUser.isEmpty()) {
			return;
		}
		
		final User user = optUser.get();
		
		final JsonObject json = HttpUtil.readJsonFromRequestBody(request, response);
		
		if (json == null) {
			return;
		}
		
		try {
			Optional<Directory> parentDirectory;

			if (json.has("parent")) {
				final String parentPath = HttpUtil.getJsonString(json, "parent", response);;
				if (parentPath == null) {
					return;
				}
				parentDirectory = Directory.findByPath(parentPath);
				if (parentDirectory.isEmpty()) {
					ApiError.DIRECTORY_NOT_EXISTS.send(response);
					return;
				}
			} else {
				parentDirectory = Optional.empty();
			}
			
			if (!user.hasWriteAccess()) {
				ApiError.MISSING_WRITE_ACCESS.send(response);
				return;
			}
			
			String name;
			try {
				name = json.get("name").getAsString();
				Validation.validateFileDirectoryName(name);
			} catch (final Exception e) {
				ApiError.INVALID_FILE_DIRECTORY_NAME.send(response);
				return;
			}
			
			Directory newDirectory;
			try {
				if (parentDirectory.isPresent()) {
					newDirectory = parentDirectory.get().createDirectory(name);
				} else {
					newDirectory = Directory.createRootDirectory(name);
				}
			} catch (final AlreadyExistsException e) {
				ApiError.DIRECTORY_ALREADY_EXISTS.send(response);
				System.out.println("NOT AN ERROR");
				e.printStackTrace();
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
