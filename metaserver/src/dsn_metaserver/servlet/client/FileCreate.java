package dsn_metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import dsn_metaserver.exception.AlreadyExistsException;
import dsn_metaserver.model.Directory;
import dsn_metaserver.model.File;
import dsn_metaserver.model.User;
import dsn_metaserver.servlet.ApiError;
import dsn_metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class FileCreate extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		Optional<User> optUser;
		try {
			optUser = ClientAuthentication.verify(request, response);
			
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
			final String dirPath = HttpUtil.getJsonString(json, "dir", response);
			final String fileName = HttpUtil.getJsonString(json, "name", response);
			if (dirPath == null || fileName == null) {
				return;
			}
		
			final Optional<Directory> optDir = Directory.findByPath(dirPath);

			if (optDir.isEmpty()) {
				ApiError.DIRECTORY_NOT_EXISTS.send(response);
				return;
			}
			
			final Directory dir = optDir.get();
			
			if (dir.containsDirectory(fileName)) {
				ApiError.IS_A_DIRECTORY.send(response);
				return;
			}
			
			if (dir.containsFile(fileName)) {
				ApiError.FILE_ALREADY_EXISTS.send(response);
				return;
			}
			
			File file;
			try {
				file = dir.createFile(fileName);
			} catch (final AlreadyExistsException e) {
				throw new IllegalStateException(e);
			}
			
			try (JsonWriter jsonResponse = HttpUtil.getJsonWriter(response)) {
				jsonResponse.beginObject();
				jsonResponse.name("file");
				FileInfo.writeFileInfoJson(file, jsonResponse);
				jsonResponse.endObject();
			}
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}

}
