package eclipfs.metaserver.http.endpoints.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import org.springframework.security.crypto.codec.Hex;

import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.http.ApiError;
import eclipfs.metaserver.http.HttpUtil;
import eclipfs.metaserver.http.endpoints.ClientApiEndpoint;
import eclipfs.metaserver.model.Chunk;
import eclipfs.metaserver.model.File;
import eclipfs.metaserver.model.User;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ChunkInfo extends ClientApiEndpoint {

	public ChunkInfo() {
		super("chunkInfo", RequestMethod.GET);
	}

	@Override
	protected void handle(final User user, final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, SQLException {
		final File file = HttpUtil.getFileInodeParameter(request, response);
		final Long chunkIndex = HttpUtil.getLongParameter(request, response, "index");

		if (file == null|| chunkIndex == null) {
			return;
		}

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
	}

}
