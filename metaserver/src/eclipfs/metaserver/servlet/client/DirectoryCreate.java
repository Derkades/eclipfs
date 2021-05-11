package eclipfs.metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.exception.AlreadyExistsException;
import eclipfs.metaserver.http.endpoints.ClientApiEndpoint;
import eclipfs.metaserver.model.Directory;
import eclipfs.metaserver.model.User;
import eclipfs.metaserver.servlet.ApiError;
import eclipfs.metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class DirectoryCreate extends ClientApiEndpoint {

	public DirectoryCreate() {
		super("directoryCreate", RequestMethod.POST);
	}

	@Override
	protected void handle(final User user, final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, SQLException {
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
	}

}
