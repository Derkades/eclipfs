package eclipfs.metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.exception.AlreadyExistsException;
import eclipfs.metaserver.model.Directory;
import eclipfs.metaserver.model.User;
import eclipfs.metaserver.servlet.ApiError;
import eclipfs.metaserver.servlet.HttpUtil;
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
			
			Directory directory;
			try {
				directory = parent.createDirectory(name);
			} catch (final AlreadyExistsException e) {
				ApiError.DIRECTORY_ALREADY_EXISTS.send(response);
				return;
			}
			
			try (JsonWriter writer = new JsonWriter(response.getWriter())) {
				writer.beginObject();
				writer.name("directory");
				writer.beginObject();
				InodeInfo.writeInodeInfoJson(directory, writer);
				writer.endObject();
				writer.endObject();
			}
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}

}
