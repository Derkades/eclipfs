package eclipfs.metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import org.springframework.security.crypto.codec.Hex;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.Nodes;
import eclipfs.metaserver.Nodes.FilterStrategy;
import eclipfs.metaserver.Replication;
import eclipfs.metaserver.TransferType;
import eclipfs.metaserver.Tunables;
import eclipfs.metaserver.Validation;
import eclipfs.metaserver.model.File;
import eclipfs.metaserver.model.OnlineNode;
import eclipfs.metaserver.model.User;
import eclipfs.metaserver.model.WritingChunk;
import eclipfs.metaserver.servlet.ApiError;
import eclipfs.metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ChunkUploadInit extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
			final Optional<User> optUser = ClientAuthentication.verify(request, response);
			if (optUser.isEmpty()) {
				return;
			}

			final JsonObject json = HttpUtil.readJsonFromRequestBody(request, response);
			if (json == null) {
				return;
			}

			final File file = HttpUtil.getJsonFile(json, response);
			final Long chunkIndex = HttpUtil.getJsonLong(json, response, "chunk");
			final String checksum = HttpUtil.getJsonString(json, response, "checksum");
			final Long size = HttpUtil.getJsonLong(json, response, "size");

			if (file == null || chunkIndex == null || checksum == null || size == null) {
				return;
			}

			final User user = optUser.get();


			if (!user.hasWriteAccess()) {
				ApiError.MISSING_WRITE_ACCESS.send(response);
				return;
			}

			Replication.signalBusy();

			final WritingChunk writing = file.createChunk(chunkIndex.intValue(), Hex.decode(checksum), size);

			List<OnlineNode> nodes;
			if (json.has("location")) {
				nodes = Nodes.selectNodes(Tunables.CHUNK_WRITE_NODES, null, TransferType.UPLOAD, FilterStrategy.SHOULD, json.get("location").getAsString());
			} else {
				nodes = Nodes.selectNodes(Tunables.CHUNK_WRITE_NODES, null, TransferType.UPLOAD);
			}

			if (nodes.size() < Tunables.CHUNK_WRITE_NODES) {
				ApiError.TEMPORARY_NODE_SHORTAGE.send(response);
				return;
			}

			try (JsonWriter writer = HttpUtil.getJsonWriter(response)) {
				writer.beginObject();
				writer.name("id").value(writing.getId());
				writer.name("nodes").beginArray();
				for (final OnlineNode node : nodes) {
					final String address = node.getAddress() +
							"/upload" +
							"?node_token=" + node.getToken(TransferType.UPLOAD) +
							"&id=" + writing.getId();
					Validation.validateUrl(address);
					writer.beginObject();
					writer.name("id").value(node.getId());
					writer.name("address").value(address);
					writer.endObject();
				}
				writer.endArray().endObject();
			}

			Replication.signalBusy();
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}
}
