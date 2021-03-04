package dsn_metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import org.springframework.security.crypto.codec.Hex;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import dsn_metaserver.Nodes;
import dsn_metaserver.TransferType;
import dsn_metaserver.Validation;
import dsn_metaserver.model.Chunk;
import dsn_metaserver.model.Directory;
import dsn_metaserver.model.File;
import dsn_metaserver.model.OnlineNode;
import dsn_metaserver.model.User;
import dsn_metaserver.servlet.ApiError;
import dsn_metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ChunkTransfer extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		final JsonObject json = HttpUtil.readJsonFromRequestBody(request, response);
		if (json == null) {
			return;
		}
		
		final String directoryPath = HttpUtil.getJsonString(json, "directory", response);
		final String fileName = HttpUtil.getJsonString(json, "file", response);
		final Long chunkIndex = HttpUtil.getJsonLong(json, "chunk", response);
		final String transferTypeString = HttpUtil.getJsonString(json, "type", response);
		
		if (directoryPath == null || fileName == null || chunkIndex == null || transferTypeString == null) {
			return;
		}
		
		try {
			final Optional<User> optUser = ClientAuthentication.verify(request, response);
			if (optUser.isEmpty()) {
				return;
			}
			
			final User user = optUser.get();
			
			TransferType transferType;
			try {
				transferType = TransferType.valueOf(transferTypeString.toUpperCase());
			} catch (final IllegalArgumentException e) {
				HttpUtil.sendBadRequest(response, "transfer type must be 'upload', 'download'");
				return;
			}
			
			if (transferType == TransferType.UPLOAD && !user.hasWriteAccess()) {
				ApiError.MISSING_WRITE_ACCESS.send(response);
				return;
			}
			
			final Optional<Directory> optDirectory = Directory.findByPath(directoryPath);
			
			if (optDirectory.isEmpty()) {
				ApiError.DIRECTORY_NOT_EXISTS.send(response);
				return;
			}
			
			final Directory directory = optDirectory.get();
			
			final Optional<File> optFile = directory.getFile(fileName);
			
			if (optFile.isEmpty()) {
				ApiError.FILE_NOT_EXISTS.send(response);
				return;
			}
			
			final File file = optFile.get();
			
			Chunk chunk;
			final Optional<Chunk> optChunk = file.getChunk(chunkIndex.intValue());
			
			if (transferType == TransferType.UPLOAD) {
				if (optChunk.isPresent()) {
					chunk = optChunk.get();
					// TODO only update checksum after successful update somehow
					final String checksum = HttpUtil.getJsonString(json, "checksum", response);
					final Long size = HttpUtil.getJsonLong(json, "size", response);
					if (checksum == null || size == null) {
						return;
					}
					chunk.updateChecksum(Hex.decode(checksum));
					chunk.updateSize(size);
//					method = "update";
				} else {
					final String checksum = HttpUtil.getJsonString(json, "checksum", response);
					final Long size = HttpUtil.getJsonLong(json, "size", response);
					if (checksum == null || size == null) {
						return;
					}
					chunk = file.createChunk(chunkIndex.intValue(), Hex.decode(checksum), size);
				}
			} else if (transferType == TransferType.DOWNLOAD) {
				if (optChunk.isPresent()) {
					chunk = optChunk.get();
				} else {
					ApiError.CHUNK_NOT_EXISTS.send(response);
					return;
				}
			} else {
				throw new IllegalStateException(transferType.name());
			}
			
			final Optional<OnlineNode> optNode = Nodes.selectNode(chunk, transferType, null, null);

			if (optNode.isEmpty()) {
				ApiError.FILE_DOWNLOAD_NODES_UNAVAILABLE.send(response);
				return;
			}
			
			final OnlineNode node = optNode.get();
			
			final String nodeToken = node.getToken(transferType);
			
			final String address = node.getAddress() +
					"/" + transferTypeString +
					"?node_token=" + nodeToken +
					"&chunk_token=" + chunk.getToken();
			
			// Sanity check on generated address
			Validation.validateUrl(address);

			try (JsonWriter writer = HttpUtil.getJsonWriter(response)) {
				writer.beginObject();
				writer.name("url").value(address);
				if (transferType == TransferType.UPLOAD) {
					writer.name("checksum").value(chunk.getChecksumHex());
				}
				writer.endObject();
			}
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}

}
