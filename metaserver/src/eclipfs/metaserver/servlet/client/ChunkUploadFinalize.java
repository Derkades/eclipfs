package eclipfs.metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import eclipfs.metaserver.http.endpoints.ClientApiEndpoint;
import eclipfs.metaserver.model.Chunk;
import eclipfs.metaserver.model.File;
import eclipfs.metaserver.model.OnlineNode;
import eclipfs.metaserver.model.User;
import eclipfs.metaserver.model.WritingChunk;
import eclipfs.metaserver.servlet.ApiError;
import eclipfs.metaserver.servlet.HttpUtil;
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

		boolean success = false;
		for (final OnlineNode node : nodes) {
			if (node.finalizeUpload(writing.getId(), chunk.getId(), LOGGER)) {
				chunk.addNode(node);
				success = true;
			}
		}

		// As long as at least one node worked, we're good
		if (success) {
			HttpUtil.writeSuccessTrueJson(response);
		} else {
			ApiError.UPLOAD_FINALIZE_FAILED.send(response);
			return;
		}
	}

}
