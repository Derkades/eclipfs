package eclipfs.metaserver;

public class Tunables {

	public static final int REPLICATION_GOAL = 2;

	public static final int MINIMUM_FREE_SPACE_FOR_UPLOAD = 50_000_000; // 50 MB

	public static final int NODE_OFFLINE_TIMEOUT = 15_000;

	public static final long REPLICATION_IDLE_WAIT = 5_000;
	public static final long REPLICATION_DELAY = 200;
	public static final int REPLICATION_ADD_AMOUNT = 5000;
	public static final int REPLICATION_EMPTY_SLEEP = 120_000;

	public static final int NODE_TOKEN_LENGTH = 32;

	public static final int CHUNK_WRITE_NODES = 2;

}
