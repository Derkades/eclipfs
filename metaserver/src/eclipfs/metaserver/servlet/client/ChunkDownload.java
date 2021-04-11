package eclipfs.metaserver.servlet.client;

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
import eclipfs.metaserver.model.Chunk;
import eclipfs.metaserver.model.File;
import eclipfs.metaserver.model.OnlineNode;
import eclipfs.metaserver.model.User;
import eclipfs.metaserver.servlet.ApiError;
import eclipfs.metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ChunkDownload extends HttpServlet {

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
//			final String transferTypeString = HttpUtil.getJsonString(json, response, "type");

			if (file == null || chunkIndex == null) {
				return;
			}

//			final User user = optUser.get();

//			TransferType transferType;
//			try {
//				transferType = TransferType.valueOf(transferTypeString.toUpperCase());
//			} catch (final IllegalArgumentException e) {
//				HttpUtil.sendBadRequest(response, "transfer type must be 'upload', 'download'");
//				return;
//			}

//			if (transferType == TransferType.UPLOAD && !user.hasWriteAccess()) {
//				ApiError.MISSING_WRITE_ACCESS.send(response);
//				return;
//			}

			Chunk chunk;
			final Optional<Chunk> optChunk = file.getChunk(chunkIndex.intValue());

//			boolean removeFromNodes = false;

//			if (transferType == TransferType.UPLOAD) {
//				if (optChunk.isPresent()) {
//					chunk = optChunk.get();
//					// TODO only update checksum after successful update somehow
//					final String checksum = HttpUtil.getJsonString(json, response, "checksum");
//					final Long size = HttpUtil.getJsonLong(json, response, "size");
//					if (checksum == null || size == null) {
//						return;
//					}
//					chunk.updateChecksum(Hex.decode(checksum));
//					chunk.updateSize(size);
//					// Transfer type 'UPLOAD' can mean uploading a new chunk, but can also mean
//					// overwriting an existing chunk. The existing chunk may be replicated on several nodes
//					// it will only be updated on one. Remove the chunk from all nodes, before allowing  the client
//					// to upload it to one node so there are no inconsistencies.
//					// defer until after some api error checks so if this api call failes the chunks are not removed
//					removeFromNodes = true;
//				} else {
//					final String checksum = HttpUtil.getJsonString(json, response, "checksum");
//					final Long size = HttpUtil.getJsonLong(json, response, "size");
//					if (checksum == null || size == null) {
//						return;
//					}
//					chunk = file.createChunk(chunkIndex.intValue(), Hex.decode(checksum), size);
//				}
//			} else if (transferType == TransferType.DOWNLOAD) {
				if (optChunk.isPresent()) {
					chunk = optChunk.get();
				} else {
					ApiError.CHUNK_NOT_EXISTS.send(response);
					return;
				}
//			} else {
//				throw new IllegalStateException(transferType.name());
//			}

			List<OnlineNode> nodes;
//			final Optional<OnlineNode> optNode;
			if (json.has("location")) {
				nodes = Nodes.selectNodes(1, chunk, TransferType.DOWNLOAD, FilterStrategy.SHOULD, json.get("location").getAsString());
			} else {
				nodes = Nodes.selectNodes(1, chunk, TransferType.DOWNLOAD);
			}

			if (nodes.isEmpty()) {
//			if (nodes.size() < Tunables.CHUNK_WRITE_NODES) {
				ApiError.FILE_DOWNLOAD_NODES_UNAVAILABLE.send(response);
				return;
			}

			Validate.isTrue(nodes.size() == 1);
			final OnlineNode node = nodes.get(0);

//			if (removeFromNodes) {
//				System.out.println("REMOVING ALL NODES FROM CHUNK " + chunk.getId());
//				chunk.removeAllNodes();
//			}

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
//				if (transferType == TransferType.DOWNLOAD) {
					writer.name("checksum").value(chunk.getChecksumHex());
//				}
				writer.endObject();
			}

			Replication.signalBusy();
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}

}
