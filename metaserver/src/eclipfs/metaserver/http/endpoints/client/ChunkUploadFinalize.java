package eclipfs.metaserver.http.endpoints.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import eclipfs.metaserver.MetaServer;
import eclipfs.metaserver.http.ApiError;
import eclipfs.metaserver.http.HttpUtil;
import eclipfs.metaserver.http.endpoints.ClientApiEndpoint;
import eclipfs.metaserver.model.Chunk;
import eclipfs.metaserver.model.File;
import eclipfs.metaserver.model.OnlineNode;
import eclipfs.metaserver.model.User;
import eclipfs.metaserver.model.WritingChunk;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ChunkUploadFinalize extends ClientApiEndpoint {

	public ChunkUploadFinalize() {
		super("chunkUploadFinalize", RequestMethod.POST);
	}

	private static final Logger LOGGER = LoggerFactory.getLogger("http - chunk upload finalize");

	@Override
	protected void handle(final User user, final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, SQLException {
		final JsonObject json = HttpUtil.readJsonFromRequestBody(request, response);
		if (json == null) {
			return;
		}

		if (!user.hasWriteAccess()) {
			ApiError.MISSING_WRITE_ACCESS.send(response);
			return;
		}

		final Long writingChunkId = HttpUtil.getJsonLong(json, response, "id");
		final long[] nodeIds = HttpUtil.getJsonLongArray(json, response, "nodes");

		if (writingChunkId == null || nodeIds == null) {
			return;
		}

		final Optional<WritingChunk> optWriting = WritingChunk.byId(writingChunkId);

		if (optWriting.isEmpty()) {
			ApiError.WRITING_CHUNK_NOT_EXISTS.send(response);
			return;
		}

		final WritingChunk writing = optWriting.get();

		final List<OnlineNode> nodes = new ArrayList<>(nodeIds.length);

		for (final long nodeId : nodeIds) {
			OnlineNode.getOnlineNodeById(nodeId).ifPresent(nodes::add);
		}

		if (nodes.isEmpty()) {
			ApiError.NOT_ENOUGH_NODES_SPECIFIED.send(response);
			return;
		}

		final File file = writing.getFile();
		file.deleteChunk(writing.getIndex());
		final Chunk chunk = writing.finalizeChunk();

		for (int i = 0; i < nodes.size(); i++) {
			final OnlineNode node = nodes.get(i);
			if (node.finalizeUpload(writing.getId(), chunk.getId(), LOGGER)) {
				chunk.addNode(node);

				// we can already respond to the client that everything is fine
				// worst case it has still uploaded to one node
				HttpUtil.writeSuccessTrueJson(response);

				final int asyncStartIndex = i + 1;

				// Start task to ping remaining nodes async
				MetaServer.getExecutorService().execute(() -> {
					for (int j = asyncStartIndex; j < nodes.size(); j++) {
						final OnlineNode node2 = nodes.get(j);
						try {
							if (node2.finalizeUpload(writing.getId(), chunk.getId(), LOGGER)) {
								chunk.addNode(node2);
							} else {
								LOGGER.warn("Failed delayed finalize");
							}
						} catch (IOException | SQLException e) {
							LOGGER.warn("Failed delayed finalize", e);
						}
					}
				});
				return;
			}
		}

		// we've tried all nodes, nothing worked
		ApiError.UPLOAD_FINALIZE_FAILED.send(response);
	}

}
