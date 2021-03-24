package eclipfs.metaserver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eclipfs.metaserver.Nodes.FilterStrategy;
import eclipfs.metaserver.model.Chunk;
import eclipfs.metaserver.model.Node;
import eclipfs.metaserver.model.OnlineNode;

public class Replication {

	private static final Logger LOGGER = LoggerFactory.getLogger("Replication");

	private static long lastBusyTime = 0;

	public static void signalBusy() {
		lastBusyTime = System.currentTimeMillis();
	}

	private static boolean isBusy() {
		return System.currentTimeMillis() - lastBusyTime < Tunables.REPLICATION_IDLE_WAIT;
	}

	// for dashboard
	public static String getStatus() {
		if (isBusy()) {
			return "Waiting (transfers in progress)";
		}

		if (queue.isEmpty()) {
			return "Idle (nothing to do)";
		}

		if (fast) {
			return "Running (quickly)";
		} else {
			return "Running (slowly)";
		}
	}

	final static Deque<Long> queue = new ArrayDeque<>();
	static boolean fast = false;

	// for dashboard
	public static int getQueueSize() {
		return queue.size();
	}

	static void run() {


		while(true) {
			try {
				Thread.sleep(fast ? Tunables.REPLICATION_FAST_DELAY : Tunables.REPLICATION_SLOW_DELAY);

				if (isBusy()) {
					continue;
				}

				if (queue.isEmpty()) {
					final long start = System.currentTimeMillis();
					addUndergoalChunks(queue, Tunables.REPLICATION_ADD_AMOUNT);
					final long time = System.currentTimeMillis() - start;
					if (queue.isEmpty()) {
						LOGGER.info("Queue still empty, going to sleep for a while (took " + time + "ms to find chunks).");
						Thread.sleep(Tunables.REPLICATION_EMPTY_SLEEP);
					} else {
						LOGGER.info("Added " + queue.size() + " chunks to the replication queue (took " + time + "ms).");
					}
					continue;
				}

				fast = queue.size() > Tunables.REPLICATION_FAST_THRESHOLD;

				LOGGER.info("Processing replication queue, " + queue.size() + " entries left.");

				final long chunkId = queue.pop();
				final Optional<Chunk> optChunk = Chunk.byId(chunkId);
				if (optChunk.isEmpty()) {
					LOGGER.warn("Skipping chunk " + chunkId + ", it has been deleted.");
					continue;
				}
				final Chunk chunk = optChunk.get();
				final List<Node> nodes = chunk.getNodes();
				final Set<String> existingLabels = nodes.stream().map(Node::getLocation).distinct().collect(Collectors.toSet());
				final int replication = existingLabels.size();
				final String chunkStr = chunk.getFile().getId() + "." + chunk.getIndex();
				if (replication > Tunables.REPLICATION_GOAL) {
					LOGGER.warn("Chunk " + chunkStr + " is overgoal");
					continue;
				} else if (replication == Tunables.REPLICATION_GOAL) {
					LOGGER.warn("Chunk " + chunkStr + " is replicated correctly");
					continue;
				}

				LOGGER.info("Chunk " + chunkStr + " is undergoal (" + replication + "/" + Tunables.REPLICATION_GOAL + ")");
				final Optional<OnlineNode> optReplicationTarget = Nodes.selectNode(chunk, TransferType.UPLOAD, FilterStrategy.MUST_NOT, existingLabels);
				if (optReplicationTarget.isEmpty()) {
					LOGGER.warn("Cannot replicate chunk, no target node available. Current labels: " + String.join(", ", existingLabels));
					continue;
				}

				final OnlineNode replicationTarget = optReplicationTarget.get();
				final Optional<OnlineNode> optReplicationSource = Nodes.selectNode(chunk, TransferType.DOWNLOAD);
				if (optReplicationSource.isEmpty()) {
					LOGGER.warn("Cannot replicate chunk, not source node available.");
					continue;
				}

				final OnlineNode replicationSource = optReplicationSource.get();
				try {
					if (replicationSource.requestReplicate(chunk, replicationTarget)) {
						LOGGER.info("Successfully replicated chunk.");
					} else {
						LOGGER.warn("Error while replicating chunk");
						continue;
					}
				} catch (final IOException e) {
					e.printStackTrace();
					LOGGER.warn("Error while replicating chunk " + e.getClass().getSimpleName() + " " + e.getMessage());
					continue;
				}
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}
	}

//	public static void addRandomChunksToQueue(final int amount) throws SQLException {
//		if (CHUNK_CHECK_QUEUE.size() > amount) {
//			LOGGER.info("Chunk queue is already quite full, not adding random chunks");
//			return;
//		}
//		try (Connection conn = Database.getConnection();
//				PreparedStatement query = conn.prepareStatement("SELECT id FROM chunk TABLESAMPLE SYSTEM_ROWS(?)")){
//			query.setInt(1, amount);
//			final ResultSet result = query.executeQuery();
//			while (result.next()) {
//				addToCheckQueue(result.getLong(1));
//			}
//		}
//	}

	public static void addUndergoalChunks(final Deque<Long> queue, final int limit) throws SQLException {
		try (Connection conn = Database.getConnection();
//				PreparedStatement query = conn.prepareStatement("SELECT id FROM chunk JOIN chunk_node ON id=chunk GROUP BY chunk.id HAVING COUNT(node) < ? LIMIT ?")) {
				PreparedStatement query = conn.prepareStatement("SELECT chunk.id \n"
						+ "FROM chunk \n"
						+ "	JOIN chunk_node ON chunk=chunk.id \n"
						+ "	JOIN node ON node=node.id \n"
						+ "GROUP BY chunk.id \n"
						+ "HAVING COUNT(DISTINCT node.location) < ? LIMIT ?")) {
				query.setInt(1, Tunables.REPLICATION_GOAL);
			query.setInt(2, limit);
			final ResultSet result = query.executeQuery();
			while (result.next()) {
				queue.add(result.getLong(1));
			}
		}
	}

}
