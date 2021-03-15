package eclipfs.metaserver;

@Deprecated
public class Transfer {
		
//	private static final Map<Node, Transfer> TRANSFERS = new HashMap<>();
	
//	private final Chunk chunk;
//	private final Node node;
//	private final long startTimestamp;
//
//	/**
//	 *
//	 * @param chunkIndex
//	 * @param chunkId
//	 * @param nodes List of nodes this chunk can be uploaded to
//	 */
//	private Transfer(final Chunk chunk, final Node node) {
//		this.chunk = chunk;
//		this.node = node;
//		this.startTimestamp = System.currentTimeMillis();
//	}
//
//	public Chunk getChunk() {
//		return this.chunk;
//	}
//
//	public Node getNode() {
//		return this.node;
//	}
//
//	public long getStartTimestamp() {
//		return this.startTimestamp;
//	}
//
//	public void finish() {
//		TRANSFERS.remove(this.node);
//	}
	
//	private static Node selectFreeDownloadNode() throws NodeShortageException {
//		// TODO Select based on download priority and free space
//		for (final Node node : Node.list()) {
//			if (TRANSFERS.containsKey(node)) {
//				continue;
//			}
//			return node;
//		}
//		throw new NodeShortageException();
//	}
//
//	private static Node selectFreeUploadNode() throws NodeShortageException {
//		// TODO Select based on upload priority (and free space?)
//		return selectFreeDownloadNode();
//	}
	
//	public static Transfer uploadChunk(final File file, final int chunkIndex) throws NodeShortageException {
//		synchronized (TRANSFERS) {
//			final Node node = selectFreeUploadNode();
//			final Chunk chunk = file.createChunk(chunkIndex);
//			final Transfer transfer = new Transfer(chunk, node);
//			TRANSFERS.put(node, transfer);
//			return transfer;
//		}
//	}
	
//	public static Transfer downloadChunk(final File file, final int chunkIndex) throws NodeShortageException {
//		synchronized (TRANSFERS) {
//			final Node node = selectFreeDownloadNode();
//			final Chunk chunk = file.getChunk(chunkIndex);
//			final Transfer transfer = new Transfer(chunk, node);
//			TRANSFERS.put(node, transfer);
//			return transfer;
//		}
//	}


}
