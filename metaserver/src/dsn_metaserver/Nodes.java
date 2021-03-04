package dsn_metaserver;

import static dsn_metaserver.Nodes.FilterStrategy.MUST;
import static dsn_metaserver.Nodes.FilterStrategy.MUST_NOT;
import static dsn_metaserver.Nodes.FilterStrategy.SHOULD;
import static dsn_metaserver.Nodes.FilterStrategy.SHOULD_NOT;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.Validate;

import dsn_metaserver.model.Chunk;
import dsn_metaserver.model.OnlineNode;

public class Nodes {
	
	// TODO intelligent algorithm etc
	
	public static Optional<OnlineNode> selectNode(final Chunk chunk, final TransferType type, final String label, final FilterStrategy strategy) throws SQLException {
		Validate.notNull(chunk, "Chunk is null");
		Validate.notNull(type, "Transfer type is null");
		if (label != null) {
			Validate.notNull(strategy, "Filter strategy must not be null if label is not null");
		}
		
		final List<OnlineNode> nodes;
		if (type == TransferType.DOWNLOAD) {
			nodes = chunk.getOnlineNodes();
		} else {
			nodes = OnlineNode.getOnlineNodes();
		}
		
		final List<OnlineNode> candidates = new ArrayList<>();
		
		for (final OnlineNode node : candidates) {
			if (labelGood(node, label, strategy)) {
				candidates.add(node);
			}
		}
		
		if (!candidates.isEmpty()) {
			return Optional.of(nodes.get(0));
		} else {
			if (strategy == SHOULD || strategy == SHOULD_NOT) {
				// it's okay if we use other nodes
				if (!nodes.isEmpty()) {
					return Optional.of(nodes.get(0));
				} else {
					return Optional.empty();
				}
			} else {
				return Optional.empty();
			}
		}
	}
	
	private static boolean labelGood(final OnlineNode node, final String label, final FilterStrategy strategy) {
		if (strategy == MUST || strategy == SHOULD) {
			return node.getLabel().equals(label);
		} else if (strategy == MUST_NOT || strategy == SHOULD_NOT) {
			return !node.getLabel().equals(label);
		}
		throw new IllegalStateException(strategy.name());
	}
	
//	public static Optional<OnlineNode> selectNodeForUpload(final Chunk chunk, final String label, final FilterStrategy strategy) {
//		Validate.notNull(chunk, "Chunk is null");
//		if (label != null) {
//			Validate.notNull(strategy, "Filter strategy must not be null if label is not null");
//		}
//
//		return Optional.of(OnlineNode.getOnlineNodes().get(0));
//	}
	
	public static enum FilterStrategy {
		MUST,
		MUST_NOT,
		SHOULD,
		SHOULD_NOT;
	}

}
