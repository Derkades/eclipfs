package eclipfs.metaserver.http.endpoints.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.springframework.security.crypto.codec.Hex;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.Nodes;
import eclipfs.metaserver.Nodes.FilterStrategy;
import eclipfs.metaserver.Replication;
import eclipfs.metaserver.TransferType;
import eclipfs.metaserver.Tunables;
import eclipfs.metaserver.Validation;
import eclipfs.metaserver.http.ApiError;
import eclipfs.metaserver.http.HttpUtil;
import eclipfs.metaserver.http.endpoints.ClientApiEndpoint;
import eclipfs.metaserver.model.File;
import eclipfs.metaserver.model.OnlineNode;
import eclipfs.metaserver.model.User;
import eclipfs.metaserver.model.WritingChunk;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ChunkUploadInit extends ClientApiEndpoint {

	public ChunkUploadInit() {
		super("chunkUploadInit", RequestMethod.POST);
	}

	@Override
	protected void handle(final User user, final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, SQLException {
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
	}
}
