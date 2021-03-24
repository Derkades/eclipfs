package eclipfs.metaserver.servlet.node;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import com.google.gson.JsonObject;

import eclipfs.metaserver.model.Chunk;
import eclipfs.metaserver.model.File;
import eclipfs.metaserver.model.Node;
import eclipfs.metaserver.servlet.ApiError;
import eclipfs.metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class NotifyChunkUploaded extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
			final Optional<Node> optNode = NodeAuthentication.verify(request, response);
			if (optNode.isEmpty()) {
				return;
			}

			final Node node = optNode.get();

			final JsonObject json = HttpUtil.readJsonFromRequestBody(request, response);
			if (json == null) {
				return;
			}

//			final String chunkToken = HttpUtil.getJsonString(json, response, "chunk_token");
			final File file = HttpUtil.getJsonFile(json, response);
			final Long chunkIndex = HttpUtil.getJsonLong(json, response, "index");
			final Long size = HttpUtil.getJsonLong(json, response, "size");
			if (file == null || chunkIndex == null || size == null) {
				return;
			}

//			final Optional<Chunk> optChunk = Chunk.findByToken(chunkToken);
			final Optional<Chunk> optChunk = file.getChunk(chunkIndex.intValue());
			if (optChunk.isEmpty()) {
				ApiError.CHUNK_NOT_EXISTS.send(response);
				return;
			}
			final Chunk chunk = optChunk.get();

			chunk.getFile().setMtime(System.currentTimeMillis());

			if (chunk.getSize() != size) {
				ApiError.SIZE_MISMATCH.send(response, "expected " + chunk.getSize() + " got " + size);
				return;
			}

			chunk.addNode(node);

//			Replication.addToCheckQueue(chunk);

			HttpUtil.writeSuccessTrueJson(response);
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}

}
