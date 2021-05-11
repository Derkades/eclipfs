package eclipfs.metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.exception.AlreadyExistsException;
import eclipfs.metaserver.http.endpoints.ClientApiEndpoint;
import eclipfs.metaserver.model.Directory;
import eclipfs.metaserver.model.File;
import eclipfs.metaserver.model.User;
import eclipfs.metaserver.servlet.ApiError;
import eclipfs.metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class FileCreate extends ClientApiEndpoint {

	public FileCreate() {
		super("fileCreate", RequestMethod.POST);
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
			writer.beginObject().name("file").beginObject();
			InodeInfo.writeInodeInfoJson(file, writer);
			writer.endObject().endObject();
		}
	}

}
