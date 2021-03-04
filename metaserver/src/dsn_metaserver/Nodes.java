package dsn_metaserver;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import dsn_metaserver.model.Chunk;
import dsn_metaserver.model.OnlineNode;

public class Nodes {
	
	// TODO intelligent algorithm etc
	
	public static Optional<OnlineNode> selectNodeForDownload(final Chunk chunk, final String label, final boolean labelStrict) throws SQLException {
		final List<OnlineNode> nodes = chunk.getOnlineNodes();
		if (nodes.size() > 0) {
			return Optional.of(nodes.get(0));
		} else {
			return Optional.empty();
		}
	}
	
	public static Optional<OnlineNode> selectNodeForUpload(final Chunk chunk, final String label, final boolean labelStrict) {
		return Optional.of(OnlineNode.getOnlineNodes().get(0));
	}

}
