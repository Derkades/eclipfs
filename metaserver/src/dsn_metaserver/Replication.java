package dsn_metaserver;

import java.util.LinkedList;
import java.util.Queue;

import dsn_metaserver.model.Chunk;

public class Replication {
	
	// voordat ik dit maak, labels toevoegen aan nodes
	
	private static final Queue<Long> CHUNK_CHECK_QUEUE = new LinkedList<>();
	
	private static long lastBusyTime = System.currentTimeMillis();

	public static void signalBusy() {
		lastBusyTime = System.currentTimeMillis();
	}
	
	public static void addToCheckQueue(final Chunk chunk) {
		CHUNK_CHECK_QUEUE.add(chunk.getId());
	}
	
	public static void start() {
		System.out.println("Starting replication");
		while (!CHUNK_CHECK_QUEUE.isEmpty()) {
			final long chunkId = CHUNK_CHECK_QUEUE.poll();
			final Optional<Chunk> optChunk = Chunk.
		}
		System.out.println("Replication completed");
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
