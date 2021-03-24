package eclipfs.metaserver;

public class Tunables {

	/**
	 * Chunk size in bytes
	 * CANNOT BE CHANGED FOR AN EXISTING FILE SYSTEM
	 */
	public static final int CHUNKSIZE = 1_000_000; // 1MB

	public static final int REPLICATION_GOAL = 2;

	public static final int MINIMUM_FREE_SPACE_FOR_UPLOAD = 50_000_000; // 50 MB

	public static final int NODE_OFFLINE_TIMEOUT = 10_000;

	public static final long REPLICATION_IDLE_WAIT = 5_000;
	public static final long REPLICATION_FAST_DELAY = 100;
	public static final long REPLICATION_SLOW_DELAY = 500;
	public static final long REPLICATION_FAST_THRESHOLD = 500;
	public static final int REPLICATION_ADD_AMOUNT = 1000;

	public static final int NODE_TOKEN_LENGTH = 32;

}
