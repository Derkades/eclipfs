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
			
			final Directory directory = HttpUtil.getJsonDirectory(json, response);
			final String name = HttpUtil.getJsonString(json, response, "name");
			if (directory == null || name == null) {
				return;
			}
			
			if (directory.contains(name)) {
				ApiError.NAME_ALREADY_EXISTS.send(response);
				return;
			}
			
			File file;
			try {
				file = directory.createFile(name);
			} catch (final AlreadyExistsException e) {
				throw new IllegalStateException(e);
			}
			
			try (JsonWriter writer = HttpUtil.getJsonWriter(response)) {
				writer.beginObject();
				writer.name("file");
				InodeInfo.writeInodeInfoJson(file, writer);
				writer.endObject();
			}
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}

}
