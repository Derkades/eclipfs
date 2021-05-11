package eclipfs.metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;

import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.MetaServer;
import eclipfs.metaserver.http.endpoints.ClientApiEndpoint;
import eclipfs.metaserver.model.User;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class GetEncryptionKey extends ClientApiEndpoint {

	public GetEncryptionKey() {
		super("getEncryptionKey", RequestMethod.GET);
	}

	@Override
	protected void handle(final User user, final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, SQLException {
		try (JsonWriter writer = new JsonWriter(response.getWriter())) {
			writer.beginObject();
			writer.name("key");
			writer.value(MetaServer.getEncryptionKeyBase64());
			writer.endObject();
		}
	}

}
