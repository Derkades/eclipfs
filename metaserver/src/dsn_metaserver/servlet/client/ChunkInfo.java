package dsn_metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import org.springframework.security.crypto.codec.Hex;

import com.google.gson.stream.JsonWriter;

import dsn_metaserver.model.Chunk;
import dsn_metaserver.model.Directory;
import dsn_metaserver.model.File;
import dsn_metaserver.servlet.ApiError;
import dsn_metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ChunkInfo extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
			if (ClientAuthentication.verify(request, response).isEmpty()) {
				return;
			}
			
			final String directoryPath = HttpUtil.getStringParameter(request, response, "directory");
			final String fileName = HttpUtil.getStringParameter(request, response, "file_name");
			final Long chunkIndex = HttpUtil.getLongParameter(request, response, "index");
			
			if (directoryPath == null || fileName == null || chunkIndex == null) {
				return;
			}
			
			final Optional<Directory> optDir = Directory.findByPath(directoryPath);
			if (optDir.isEmpty()) {
				ApiError.DIRECTORY_NOT_EXISTS.send(response);
				return;
			}
			final Directory directory = optDir.get();
			
			final Optional<File> optFile = directory.getFile(fileName);
			if (optFile.isEmpty()) {
				ApiError.FILE_NOT_EXISTS.send(response);
				return;
			}
			final File file = optFile.get();
			
			final Optional<Chunk> optChunk = file.getChunk(chunkIndex.intValue());
			if (optChunk.isEmpty()) {
				ApiError.CHUNK_NOT_EXISTS.send(response);
				return;
			}
			final Chunk chunk = optChunk.get();
			
			try (JsonWriter writer = HttpUtil.getJsonWriter(response)) {
				writer.beginObject();
				writer.name("checksum").value(new String(Hex.encode(chunk.getChecksum())));
				writer.endObject();
			}
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}

}
