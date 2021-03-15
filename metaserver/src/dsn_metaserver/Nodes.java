package dsn_metaserver;

import static dsn_metaserver.Nodes.FilterStrategy.MUST;
import static dsn_metaserver.Nodes.FilterStrategy.MUST_NOT;
import static dsn_metaserver.Nodes.FilterStrategy.SHOULD;
import static dsn_metaserver.Nodes.FilterStrategy.SHOULD_NOT;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;

import dsn_metaserver.model.Chunk;
import dsn_metaserver.model.OnlineNode;

public class Nodes {
	
	// TODO intelligent algorithm etc
	
	public static Optional<OnlineNode> selectNode(final Chunk chunk, final TransferType type) throws SQLException {
		final List<OnlineNode> nodes = getAllNodes(chunk, type);
		if (nodes.isEmpty()) {
			return Optional.empty();
		} else {
			return Optional.of(nodes.get(0));
		}
	}
	
	public static Optional<OnlineNode> selectNode(final Chunk chunk, final TransferType type, final FilterStrategy strategy, final String label) throws SQLException {
		final List<OnlineNode> nodes = getAllNodes(chunk, type);
		final List<OnlineNode> candidates = nodes.stream().filter(node -> labelGood(node, label, strategy)).collect(Collectors.toList());
		return selectFromCandidates(strategy, nodes, candidates);
	}
	
	public static Optional<OnlineNode> selectNode(final Chunk chunk, final TransferType type, final FilterStrategy strategy, final Set<String> labels) throws SQLException {
		final List<OnlineNode> nodes = getAllNodes(chunk, type);
		final List<OnlineNode> candidates = nodes.stream().filter(node -> labelsGood(node, labels, strategy)).collect(Collectors.toList());
		return selectFromCandidates(strategy, nodes, candidates);
	}
	
	private static List<OnlineNode> getAllNodes(final Chunk chunk, final TransferType type) throws SQLException {
		Validate.notNull(chunk, "Chunk is null");
		Validate.notNull(type, "Transfer type is null");
		if (type == TransferType.DOWNLOAD) {
			return chunk.getOnlineNodes();
		} else {
			return OnlineNode.getOnlineNodes();
		}
	}
	
	private static Optional<OnlineNode> selectFromCandidates(final FilterStrategy strategy, final List<OnlineNode> allNodes, final List<OnlineNode> candidates) {
		Validate.notNull(strategy);
		Validate.notNull(allNodes);
		Validate.notNull(candidates);
		if (!candidates.isEmpty()) {
			return Optional.of(candidates.get(0));
		} else {
			if (strategy == SHOULD || strategy == SHOULD_NOT) {
				// it's okay if we use other nodes
				if (!allNodes.isEmpty()) {
					return Optional.of(allNodes.get(0));
				} else {
					return Optional.empty();
				}
			} else {
				return Optional.empty();
			}
		}
	}
		
	private static boolean labelGood(final OnlineNode node, final String label, final FilterStrategy strategy) {
		Validate.notNull(node);
		Validate.notNull(label);
		Validate.notNull(strategy);
		if (strategy == MUST || strategy == SHOULD) {
			return label.equals(node.getLabel());
		} else if (strategy == MUST_NOT || strategy == SHOULD_NOT) {
			return !label.equals(node.getLabel());
		}
		throw new IllegalStateException(strategy.name());
	}
	
	private static boolean labelsGood(final OnlineNode node, final Set<String> labels, final FilterStrategy strategy) {
		Validate.notNull(node);
		Validate.notNull(labels);
		Validate.notNull(strategy);
		if (strategy == MUST || strategy == SHOULD) {
			return labels.contains(node.getLabel());
		} else if (strategy == MUST_NOT || strategy == SHOULD_NOT) {
			return !labels.contains(node.getLabel());
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
