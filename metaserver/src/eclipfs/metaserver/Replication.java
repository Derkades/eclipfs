package eclipfs.metaserver;

import java.io.IOException;
import java.sql.SQLException;
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
	
	private static final Queue<Long> CHUNK_CHECK_QUEUE = new LinkedList<>();
	
	private static long lastBusyTime = System.currentTimeMillis();

	public static void signalBusy() {
		lastBusyTime = System.currentTimeMillis();
	}
	
	public static void addToCheckQueue(final Chunk chunk) {
		CHUNK_CHECK_QUEUE.add(chunk.getId());
	}
	
	public static void addToCheckQueue(final long chunkId) {
		CHUNK_CHECK_QUEUE.add(chunkId);
	}
	
	public static void start() throws SQLException {
		LOGGER.info("Starting replication");
		int maxAmount = 100;
		while (!CHUNK_CHECK_QUEUE.isEmpty()) {
			if (maxAmount < 0) {
				LOGGER.info("Breaking replication loop, too many tries.");
				break;
			}
			maxAmount--;
			
			final long chunkId = CHUNK_CHECK_QUEUE.poll();
			final Optional<Chunk> optChunk = Chunk.byId(chunkId);
			if (optChunk.isEmpty()) {
				LOGGER.warning("Skipping chunk " + chunkId + ", it has been deleted.");
				return;
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
				CHUNK_CHECK_QUEUE.add(chunkId);
				continue;
			}
			
			final OnlineNode replicationTarget = optReplicationTarget.get();
			final Optional<OnlineNode> optReplicationSource = Nodes.selectNode(chunk, TransferType.DOWNLOAD);
			if (optReplicationSource.isEmpty()) {
				LOGGER.warning("Cannot replicate chunk, not source node available. Adding to the queue for later..");
				CHUNK_CHECK_QUEUE.add(chunkId);
				continue;
			}
			final OnlineNode replicationSource = optReplicationSource.get();
			try {
				if (replicationSource.requestReplicate(chunk, replicationTarget)) {
					LOGGER.info("Successfully replicated chunk.");
				} else {
					LOGGER.warning("Error while replicating chunk, trying again later.");
					CHUNK_CHECK_QUEUE.add(chunkId);
				}
			} catch (final IOException e) {
				e.printStackTrace();
				LOGGER.warning("Error while replicating chunk, trying again later. " + e.getClass().getSimpleName() + " " + e.getMessage());
				CHUNK_CHECK_QUEUE.add(chunkId);
			}
		}
		LOGGER.info("Replication completed");
	}
	
//	private static class ChunkQueueObj {
//
//		long fileId;
//		int chunkIndex;
//
//		ChunkQueueObj(private long fileId, private int chunkIndex){
//			this.fileId = fileId;
//			this.chunkIndex = chunkIndex;
//		}
//
//	}

}
