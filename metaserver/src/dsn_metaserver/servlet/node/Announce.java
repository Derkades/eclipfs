package dsn_metaserver.servlet.node;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Optional;

import com.google.gson.JsonObject;

import dsn_metaserver.exception.NodeNotFoundException;
import dsn_metaserver.model.Node;
import dsn_metaserver.model.OnlineNode;
import dsn_metaserver.servlet.ApiError;
import dsn_metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class Announce extends HttpServlet {
	
	private static final long serialVersionUID = 1L;

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
			final JsonObject json = HttpUtil.readJsonFromRequestBody(request, response);
			if (json == null) {
				return;
			}
			
			final Optional<Node> optNode = NodeAuthentication.verify(request, response);
			if (optNode.isEmpty()) {
				return;
			}
			
			final String token = optNode.get().getFullToken();
			final String version = HttpUtil.getJsonString(json, response, "version");
			final Long freeSpace = HttpUtil.getJsonLong(json, response, "free");
			final URL address = HttpUtil.getJsonAddress(json, response, "address");
			final Long storageQuota = HttpUtil.getJsonLong(json, response, "quota");
			final String label = HttpUtil.getJsonString(json, response, "label");
//			final String name = HttpUtil.getJsonString(json, "name", response);
			
			if (token == null ||
					version == null ||
					freeSpace == null ||
					address == null ||
					storageQuota == null ||
					label == null) {
				return;
			}
			
			try {
				OnlineNode.processNodeAnnounce(token, version, freeSpace, address, storageQuota, label);
			} catch (final NodeNotFoundException e) {
				// This is impossible, authentication check should have failed before if no node exists with this token
				throw new IllegalStateException(e);
			}
			
			// Try to make request back to node
			try {
				final HttpURLConnection connection = (HttpURLConnection) new URL(address, "/ping?node_token=" + token).openConnection();
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
	
			final JsonObject jsonResponse = new JsonObject();
			jsonResponse.addProperty("success", true);
			response.setContentType("application/json");
			response.getWriter().write(jsonResponse.toString());
		} catch (final SQLException e) {
			response.setStatus(500);
			response.setContentType("text/plain");
			response.getWriter().write("Database error");
		}
	}

}
