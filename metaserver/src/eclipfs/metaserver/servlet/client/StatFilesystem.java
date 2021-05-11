package eclipfs.metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.Tunables;
import eclipfs.metaserver.http.endpoints.ClientApiEndpoint;
import eclipfs.metaserver.model.Chunk;
import eclipfs.metaserver.model.OnlineNode;
import eclipfs.metaserver.model.User;
import eclipfs.metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class StatFilesystem extends ClientApiEndpoint {

	public StatFilesystem() {
		super("statFilesystem", RequestMethod.GET);
	}

	@Override
	protected void handle(final User user, final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, SQLException {
		// TODO Cache
		final List<OnlineNode> onlineNodes = OnlineNode.getOnlineNodes();
		final long freeSpace;
		if (onlineNodes.size() <= 1) {
			freeSpace = 0;
		} else if (onlineNodes.size() == 2) {
			freeSpace = Math.min(onlineNodes.get(0).getFreeSpace(), onlineNodes.get(1).getFreeSpace());
		} else {
			freeSpace = onlineNodes.stream().mapToLong(OnlineNode::getFreeSpace).sum() / Tunables.REPLICATION_GOAL;
		}
		final long usedSpace = Chunk.getTotalSizeEstimate();

		try (JsonWriter writer = HttpUtil.getJsonWriter(response)) {
			writer.beginObject();
			writer.name("free").value(freeSpace);
			writer.name("used").value(usedSpace);
			writer.endObject();
		}
	}

}
