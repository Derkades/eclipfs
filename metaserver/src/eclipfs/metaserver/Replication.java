package eclipfs.metaserver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import eclipfs.metaserver.Nodes.FilterStrategy;
import eclipfs.metaserver.model.Chunk;
import eclipfs.metaserver.model.OnlineNode;

public class Replication {
	
	private static final Logger LOGGER = Logger.getLogger("Replication");
	
	private static final Set<Long> DUPE_CHECK = new HashSet<>();
	private static final Queue<Long> CHUNK_CHECK_QUEUE = new LinkedList<>();
	
	private static long lastBusyTime = 0;

	public static void signalBusy() {
		lastBusyTime = System.currentTimeMillis();
	}
	
	public static void addToCheckQueue(final Chunk chunk) {
		addToCheckQueue(chunk.getId());
	}
	
	public static void addToCheckQueue(final long chunkId) {
		if (!DUPE_CHECK.contains(chunkId)) {
			DUPE_CHECK.add(chunkId);
			CHUNK_CHECK_QUEUE.add(chunkId);
		}
	}
	
	private static boolean isBusy() {
		return System.currentTimeMillis() - lastBusyTime < Tunables.REPLICATION_IDLE_WAIT;
	}
	
	static void run() {
		while(true) {
			try {
				if (CHUNK_CHECK_QUEUE.size() > 100) {
					Thread.sleep(100);
				} else {
					Thread.sleep(500);
				}
				
				if (isBusy()) {
					continue;
				}
				
				if (CHUNK_CHECK_QUEUE.isEmpty()) {
					continue;
				}
				
				LOGGER.info("Processing replication queue, " + CHUNK_CHECK_QUEUE.size() + " entries left.");
				
				final long chunkId = CHUNK_CHECK_QUEUE.poll();
				DUPE_CHECK.remove(chunkId);
				final Optional<Chunk> optChunk = Chunk.byId(chunkId);
				if (optChunk.isEmpty()) {
					LOGGER.warning("Skipping chunk " + chunkId + ", it has been deleted.");
					continue;
				}
				final Chunk chunk = optChunk.get();
				final List<OnlineNode> nodes = chunk.getOnlineNodes();
				final Set<String> existingLabels = nodes.stream().map(OnlineNode::getLabel).distinct().collect(Collectors.toSet());
				final int replication = existingLabels.size();
				final String chunkStr = chunk.getFile().getId() + "." + chunk.getIndex();
				if (replication > Tunables.REPLICATION_GOAL) {
					LOGGER.info("Chunk " + chunkStr + " is overgoal");
					continue;
				} else if (replication == Tunables.REPLICATION_GOAL) {
					LOGGER.info("Chunk " + chunkStr + " is replicated correctly");
					continue;
				}
				
				LOGGER.info("Chunk " + chunkStr + " is undergoal (" + replication + "/" + Tunables.REPLICATION_GOAL + ")");
				final Optional<OnlineNode> optReplicationTarget = Nodes.selectNode(chunk, TransferType.UPLOAD, FilterStrategy.MUST_NOT, existingLabels);
				if (optReplicationTarget.isEmpty()) {
					LOGGER.warning("Cannot replicate chunk, no target node available. Current labels: " + String.join(", ", existingLabels) + "Adding to the queue for later..");
					addToCheckQueue(chunkId);
					continue;
				}
				
				final OnlineNode replicationTarget = optReplicationTarget.get();
				final Optional<OnlineNode> optReplicationSource = Nodes.selectNode(chunk, TransferType.DOWNLOAD);
				if (optReplicationSource.isEmpty()) {
					LOGGER.warning("Cannot replicate chunk, not source node available. Adding to the queue for later..");
					addToCheckQueue(chunkId);
					continue;
				}
				
				final OnlineNode replicationSource = optReplicationSource.get();
				try {
					if (replicationSource.requestReplicate(chunk, replicationTarget)) {
						LOGGER.info("Successfully replicated chunk.");
					} else {
						LOGGER.warning("Error while replicating chunk, trying again later.");
						addToCheckQueue(chunkId);
						continue;
					}
				} catch (final IOException e) {
					e.printStackTrace();
					LOGGER.warning("Error while replicating chunk, trying again later. " + e.getClass().getSimpleName() + " " + e.getMessage());
					addToCheckQueue(chunkId);
					continue;
				}
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	static void timer() {
		try {
//			addRandomChunksToQueue(20);
			addChunksSmart();
		} catch (final SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void addRandomChunksToQueue(final int amount) throws SQLException {
		if (CHUNK_CHECK_QUEUE.size() > amount) {
			LOGGER.info("Chunk queue is already quite full, not adding random chunks");
			return;
		}
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT id FROM chunk TABLESAMPLE SYSTEM_ROWS(?)")){
			query.setInt(1, amount);
			final ResultSet result = query.executeQuery();
			while (result.next()) {
				addToCheckQueue(result.getLong(1));
			}
		}
	}
	
	public static void addAllChunksToQueue() throws SQLException {
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT id FROM chunk")){
			final ResultSet result = query.executeQuery();
			while (result.next()) {
				System.out.println(result.getLong(1));
				addToCheckQueue(result.getLong(1));
			}
		}
	}
	
	public static void addChunksSmart() throws SQLException {
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT id FROM chunk JOIN chunk_node ON id=chunk GROUP BY chunk.id HAVING COUNT(node) < ?")) {
			query.setInt(1, Tunables.REPLICATION_GOAL);
			final ResultSet result = query.executeQuery();
			while (result.next()) {
				System.out.println(result.getLong(1));
				addToCheckQueue(result.getLong(1));
			}
		}
	}

}
