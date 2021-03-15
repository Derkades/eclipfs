package eclipfs.metaserver;

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
	
	public static void start() throws SQLException {
		LOGGER.info("Starting replication");
		while (!CHUNK_CHECK_QUEUE.isEmpty()) {
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
			final String chunkStr = chunk.getFile().getId() + "." + chunk.getId();
			if (replication > Tunables.REPLICATION_GOAL) {
				LOGGER.info("Chunk " + chunkStr + " is overgoal");
				continue;
			} else if (replication == Tunables.REPLICATION_GOAL) {
				LOGGER.info("Chunk " + chunkStr + " is replicated correctly");
				continue;
			}
			
			LOGGER.info("Chunk " + chunkStr + " is undergoal");
			final Optional<OnlineNode> optReplicationTargetNode = Nodes.selectNode(chunk, TransferType.UPLOAD, FilterStrategy.MUST_NOT, existingLabels);
			if (optReplicationTargetNode.isEmpty()) {
				LOGGER.warning("Cannot replicate chunk, not enough available nodes");
				continue;
			}
			final OnlineNode replicationTargetNode = optReplicationTargetNode.get();
			final Optional<OnlineNode> optReplicationSourceNode = Nodes.selectNode(chunk, TransferType.DOWNLOAD);
			// TODO replicate
			throw new UnsupportedOperationException();
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
