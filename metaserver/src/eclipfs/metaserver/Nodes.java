package eclipfs.metaserver;

import static eclipfs.metaserver.Nodes.FilterStrategy.MUST;
import static eclipfs.metaserver.Nodes.FilterStrategy.MUST_NOT;
import static eclipfs.metaserver.Nodes.FilterStrategy.SHOULD;
import static eclipfs.metaserver.Nodes.FilterStrategy.SHOULD_NOT;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;

import eclipfs.metaserver.model.Chunk;
import eclipfs.metaserver.model.Node;
import eclipfs.metaserver.model.OnlineNode;
import xyz.derkades.derkutils.ListUtils;

public class Nodes {

	public static Optional<OnlineNode> selectNode(final Chunk chunk, final TransferType type) throws SQLException{
		return ListUtils.toOptional(selectNodes(1, chunk, type));
	}

	public static Optional<OnlineNode> selectNode(final Chunk chunk, final TransferType type, final FilterStrategy strategy, final String label) throws SQLException{
		return ListUtils.toOptional(selectNodes(1, chunk, type, strategy, label));
	}

	public static Optional<OnlineNode> selectNode(final Chunk chunk, final TransferType type, final FilterStrategy strategy, final Set<String> labels) throws SQLException{
		return ListUtils.toOptional(selectNodes(1, chunk, type, strategy, labels));
	}

	public static List<OnlineNode> selectNodes(final int count, final Chunk chunk, final TransferType type) throws SQLException {
		return selectNodes(count, chunk, type, FilterStrategy.SHOULD, node -> true);
	}

	public static List<OnlineNode> selectNodes(final int count, final Chunk chunk, final TransferType type, final FilterStrategy strategy, final String label) throws SQLException {
//		final List<OnlineNode> nodes = getAllNodes(chunk, type);
//		final List<OnlineNode> candidates = nodes.stream().filter(node -> locationGood(node, label, strategy)).collect(Collectors.toList());
		return selectNodes(count, chunk, type, strategy, node -> locationGood(node, label, strategy));
	}

	public static List<OnlineNode> selectNodes(final int count, final Chunk chunk, final TransferType type, final FilterStrategy strategy, final Set<String> labels) throws SQLException {
//		final List<OnlineNode> nodes = getAllNodes(chunk, type);
//		final List<OnlineNode> candidates = nodes.stream().filter(node -> locationsGood(node, labels, strategy)).collect(Collectors.toList());
		return selectNodes(count, chunk, type, strategy, node -> locationsGood(node, labels, strategy));
	}

//	private static List<OnlineNode> getAllNodes(final Chunk chunk, final TransferType type) throws SQLException {
//		Validate.notNull(chunk, "Chunk is null");
//		Validate.notNull(type, "Transfer type is null");
//
//	}

	private static List<OnlineNode> selectNodes(final int count, final Chunk chunk, final TransferType type, final FilterStrategy strategy, final Predicate<OnlineNode> predicate) throws SQLException {
		if (count == 0) {
			return Collections.emptyList();
		}

		final List<OnlineNode> allNodes;
		if (type == TransferType.DOWNLOAD) {
			allNodes = new ArrayList<>(chunk.getOnlineNodes()); // needs to be mutable
		} else {
			allNodes = OnlineNode.getOnlineNodes().stream().filter(n -> n.getFreeSpace() > Tunables.MINIMUM_FREE_SPACE_FOR_UPLOAD).collect(Collectors.toCollection(ArrayList::new));
		}

		Collections.shuffle(allNodes);

//		final List<OnlineNode> candidates = allNodes.stream().filter(predicate).collect(Collectors.toUnmodifiableList());

		final List<OnlineNode> finalSelection = new ArrayList<>(count);
		final Deque<OnlineNode> fallback = new ArrayDeque<>(allNodes.size());

		// First try to add the best nodes
		for (final OnlineNode node : allNodes) {
			if (!predicate.test(node)) {
				fallback.add(node);
				continue;
			}

			finalSelection.add(node);
			if (finalSelection.size() >= count) {
				return Collections.unmodifiableList(finalSelection);
			}
		}

		// Is it okay if we use other nodes?
		if (strategy == SHOULD || strategy == SHOULD_NOT) {
			while(finalSelection.size() < count && !fallback.isEmpty()) {
				finalSelection.add(fallback.pop());
			}
		}

		return Collections.unmodifiableList(finalSelection);

//		for (final OnlineNode node : allNodes) {
//
//		}
//
//		if (!candidates.isEmpty()) {
//			final int i = ThreadLocalRandom.current().nextInt(0, candidates.size());
//			return Optional.of(candidates.get(i));
//		} else {
//
//				if (!allNodes.isEmpty()) {
//					final int i = ThreadLocalRandom.current().nextInt(0, allNodes.size());
//					return Optional.of(allNodes.get(i));
//				} else {
//					return Collections.emptyList();
//				}
//			} else {
//				return Collections.emptyList();
//			}
//		}
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
