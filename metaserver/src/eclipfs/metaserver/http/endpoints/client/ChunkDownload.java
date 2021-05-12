package eclipfs.metaserver.http.endpoints.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.Validate;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.Nodes;
import eclipfs.metaserver.Nodes.FilterStrategy;
import eclipfs.metaserver.Replication;
import eclipfs.metaserver.TransferType;
import eclipfs.metaserver.Validation;
import eclipfs.metaserver.http.ApiError;
import eclipfs.metaserver.http.HttpUtil;
import eclipfs.metaserver.http.endpoints.ClientApiEndpoint;
import eclipfs.metaserver.model.Chunk;
import eclipfs.metaserver.model.File;
import eclipfs.metaserver.model.OnlineNode;
import eclipfs.metaserver.model.User;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ChunkDownload extends ClientApiEndpoint {

	public ChunkDownload() {
		super("chunkDownload", RequestMethod.POST);
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

		if (file == null || chunkIndex == null) {
			return;
		}

		Chunk chunk;
		final Optional<Chunk> optChunk = file.getChunk(chunkIndex.intValue());


		if (optChunk.isPresent()) {
			chunk = optChunk.get();
		} else {
			ApiError.CHUNK_NOT_EXISTS.send(response);
			return;
		}

		List<OnlineNode> nodes;
		if (json.has("location")) {
			nodes = Nodes.selectNodes(1, chunk, TransferType.DOWNLOAD, FilterStrategy.SHOULD, json.get("location").getAsString());
		} else {
			nodes = Nodes.selectNodes(1, chunk, TransferType.DOWNLOAD);
		}

		if (nodes.isEmpty()) {
			ApiError.FILE_DOWNLOAD_NODES_UNAVAILABLE.send(response);
			return;
		}

		Validate.isTrue(nodes.size() == 1);
		final OnlineNode node = nodes.get(0);

		final String nodeToken = node.getToken(TransferType.DOWNLOAD);

		final String address = node.getAddress() +
				"/download" +
				"?node_token=" + nodeToken +
				"&chunk=" + chunk.getId();

		// Sanity check on generated address
		Validation.validateUrl(address);

		try (JsonWriter writer = HttpUtil.getJsonWriter(response)) {
			writer.beginObject();
			writer.name("url").value(address);
			writer.name("checksum").value(chunk.getChecksumHex());
			writer.endObject();
		}

		Replication.signalBusy();
	}

}
