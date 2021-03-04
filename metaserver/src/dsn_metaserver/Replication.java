package dsn_metaserver;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.logging.Logger;

import dsn_metaserver.model.Chunk;
import dsn_metaserver.model.OnlineNode;

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
	
	private static int calculateReplication(final List<OnlineNode> nodes) {
		nodes.stream().map(OnlineNode::getLabel)
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
