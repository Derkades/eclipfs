package eclipfs.metaserver.servlet.node;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import com.google.gson.JsonObject;

import eclipfs.metaserver.model.Chunk;
import eclipfs.metaserver.model.Node;
import eclipfs.metaserver.servlet.ApiError;
import eclipfs.metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class NotifyChunkUploaded  extends HttpServlet {
	
	private static final long serialVersionUID = 1L;

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
			System.out.println("TEST");
			final Optional<Node> optNode = NodeAuthentication.verify(request, response);
			if (optNode.isEmpty()) {
				return;
			}
			
			final Node node = optNode.get();
			
			final JsonObject json = HttpUtil.readJsonFromRequestBody(request, response);
			if (json == null) {
				return;
			}
				
			final String chunkToken = HttpUtil.getJsonString(json, response, "chunk_token");
			final Long chunkSize = HttpUtil.getJsonLong(json, response, "chunk_size");
			if (chunkToken == null || chunkSize == null) {
				return;
			}
			
			final Optional<Chunk> optChunk = Chunk.findByToken(chunkToken);
			if (optChunk.isEmpty()) {
				ApiError.CHUNK_NOT_EXISTS.send(response);
				return;
			}
			final Chunk chunk = optChunk.get();
			
			if (chunk.getSize() != chunkSize) {
				ApiError.SIZE_MISMATCH.send(response, "expected " + chunk.getSize() + " got " + chunkSize);
				return;
			}
			
			chunk.addNode(node);
	
			final JsonObject jsonResponse = new JsonObject();
			jsonResponse.addProperty("success", true);
			response.setContentType("application/json");
			response.getWriter().write(jsonResponse.toString());
		} catch (final SQLException e) {
			response.setStatus(500);
			response.setContentType("text/plain");
			response.getWriter().write("Database error");
		}
	}

}
