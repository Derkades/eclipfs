package eclipfs.metaserver;

import static eclipfs.metaserver.Nodes.FilterStrategy.MUST;
import static eclipfs.metaserver.Nodes.FilterStrategy.MUST_NOT;
import static eclipfs.metaserver.Nodes.FilterStrategy.SHOULD;
import static eclipfs.metaserver.Nodes.FilterStrategy.SHOULD_NOT;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;

import eclipfs.metaserver.model.Chunk;
import eclipfs.metaserver.model.Node;
import eclipfs.metaserver.model.OnlineNode;

public class Nodes {

	public static Optional<OnlineNode> selectNode(final Chunk chunk, final TransferType type) throws SQLException {
		final List<OnlineNode> nodes = getAllNodes(chunk, type);

		if (nodes.isEmpty()) {
			return Optional.empty();
		} else {
			final int i = ThreadLocalRandom.current().nextInt(0, nodes.size());
			return Optional.of(nodes.get(i));
		}
	}

	public static Optional<OnlineNode> selectNode(final Chunk chunk, final TransferType type, final FilterStrategy strategy, final String label) throws SQLException {
		final List<OnlineNode> nodes = getAllNodes(chunk, type);
		final List<OnlineNode> candidates = nodes.stream().filter(node -> locationGood(node, label, strategy)).collect(Collectors.toList());
		return selectFromCandidates(strategy, nodes, candidates);
	}

	public static Optional<OnlineNode> selectNode(final Chunk chunk, final TransferType type, final FilterStrategy strategy, final Set<String> labels) throws SQLException {
		final List<OnlineNode> nodes = getAllNodes(chunk, type);
		final List<OnlineNode> candidates = nodes.stream().filter(node -> locationsGood(node, labels, strategy)).collect(Collectors.toList());
		return selectFromCandidates(strategy, nodes, candidates);
	}

	private static List<OnlineNode> getAllNodes(final Chunk chunk, final TransferType type) throws SQLException {
		Validate.notNull(chunk, "Chunk is null");
		Validate.notNull(type, "Transfer type is null");
		if (type == TransferType.DOWNLOAD) {
			return chunk.getOnlineNodes();
		} else {
			return OnlineNode.getOnlineNodes().stream().filter(n -> n.getFreeSpace() > Tunables.MINIMUM_FREE_SPACE_FOR_UPLOAD).collect(Collectors.toList());
		}
	}

	private static Optional<OnlineNode> selectFromCandidates(final FilterStrategy strategy, final List<OnlineNode> allNodes, final List<OnlineNode> candidates) {
		Validate.notNull(strategy);
		Validate.notNull(allNodes);
		Validate.notNull(candidates);
		if (!candidates.isEmpty()) {
			final int i = ThreadLocalRandom.current().nextInt(0, candidates.size());
			return Optional.of(candidates.get(i));
		} else {
			if (strategy == SHOULD || strategy == SHOULD_NOT) {
				// it's okay if we use other nodes
				if (!allNodes.isEmpty()) {
					final int i = ThreadLocalRandom.current().nextInt(0, allNodes.size());
					return Optional.of(allNodes.get(i));
				} else {
					return Optional.empty();
				}
			} else {
				return Optional.empty();
			}
		}
	}

	private static boolean locationGood(final Node node, final String label, final FilterStrategy strategy) {
		Validate.notNull(node);
		Validate.notNull(label);
		Validate.notNull(strategy);
		if (strategy == MUST || strategy == SHOULD) {
			return label.equals(node.getLocation());
		} else if (strategy == MUST_NOT || strategy == SHOULD_NOT) {
			return !label.equals(node.getLocation());
		}
		throw new IllegalStateException(strategy.name());
	}

	private static boolean locationsGood(final Node node, final Set<String> labels, final FilterStrategy strategy) {
		Validate.notNull(node);
		Validate.notNull(labels);
		Validate.notNull(strategy);
		if (strategy == MUST || strategy == SHOULD) {
			return labels.contains(node.getLocation());
		} else if (strategy == MUST_NOT || strategy == SHOULD_NOT) {
			return !labels.contains(node.getLocation());
		}
		throw new IllegalStateException(strategy.name());
	}

	public enum FilterStrategy {
		MUST,
		MUST_NOT,
		SHOULD,
		SHOULD_NOT;
	}

}
