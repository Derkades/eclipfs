package eclipfs.metaserver.servlet.node;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import com.google.gson.JsonObject;

import eclipfs.metaserver.http.endpoints.NodeApiEndpoint;
import eclipfs.metaserver.model.Node;
import eclipfs.metaserver.model.OnlineNode;
import eclipfs.metaserver.servlet.ApiError;
import eclipfs.metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class Announce extends NodeApiEndpoint {

	public Announce() {
		super("announce", RequestMethod.POST);
	}

	@Override
	protected void handle(final Node node, final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, SQLException {
		final JsonObject json = HttpUtil.readJsonFromRequestBody(request, response);
		if (json == null) {
			return;
		}

		final String version = HttpUtil.getJsonString(json, response, "version");
		final URL address = HttpUtil.getJsonAddress(json, response, "address");
		final Long freeSpace = HttpUtil.getJsonLong(json, response, "free");
		final Long storageQuota = HttpUtil.getJsonLong(json, response, "quota");

		if (version == null ||
				freeSpace == null ||
				address == null ||
				storageQuota == null) {
			return;
		}

		// Try to make request back to node
		try {
			final HttpURLConnection connection = (HttpURLConnection) new URL(address, "/ping?node_token=" + node.getToken()).openConnection();
			connection.setConnectTimeout(500);
			connection.setReadTimeout(500);
			if (connection.getResponseCode() != 200) {
				throw new IOException("Got HTTP response code " + connection.getResponseCode());
			}

			final byte[] content = connection.getInputStream().readAllBytes();
			if (content.length != 4) {
				throw new IOException("Content byte length != 4");
			}

			if (!new String(content, StandardCharsets.UTF_8).equals("pong")) {
				throw new IOException("Reponse != 'pong'");
			}
		} catch (final IOException e) {
			ApiError.NODE_ADDRESS_UNREACHABLE.send(response, e.toString());
			return;
		}

		OnlineNode.processNodeAnnounce(node, address, version, freeSpace, storageQuota);

		HttpUtil.writeSuccessTrueJson(response);
	}

}
