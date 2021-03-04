package dsn_metaserver.model;

@Deprecated
public class ChunkReplacement {
	
//	private final long fileId;
//	private final int index;
//	private final String token;
//
//	ChunkReplacement(final long fileId, final int index, final String token) {
//		Validate.notNull(token, "Token is null");
//		this.fileId = fileId;
//		this.index = index;
//		this.token = token;
//	}
//
//	public Chunk replace() throws SQLException {
//		synchronized(Chunk.class) {
//			final File file = File.byId(this.fileId).orElseThrow(() -> new IllegalStateException("File no longer exists"));
//			file.deleteChunk(this.index);
//
//			long chunkReplacementId;
//			byte[] checksum;
//			try (PreparedStatement query = Database.prepareStatement("SELECT id,checksum FROM chunk_replacement WHERE token=?")) {
//				final ResultSet result = query.executeQuery();
//				if (!result.next()) {
//					throw new IllegalStateException("Chunk replacement " + this.token + " no longer exists");
//				}
//				chunkReplacementId = result.getLong(1);
//				checksum = result.getBytes(2);
//			}
//
//			final Chunk oldChunk = file.getChunk(this.index).orElseThrow(() -> new IllegalStateException("Original chunk no longer exists"));
//
//			try (PreparedStatement query = Database.prepareStatement("UPDATE \"chunk\" SET size IS NULL, checksum=?, token=? WHERE id=?")) {
//				query.setBytes(1, checksum);
//				query.setString(2, this.token);
//				query.setLong(3, oldChunk.getId());
//				query.execute();
//			}
//
//			try (PreparedStatement query = Database.prepareStatement("DELETE FROM chunk_replacement WHERE id=?")) {
//				query.setLong(1, chunkReplacementId);
//				query.execute();
//			}
//
//			return file.getChunk(this.index).orElseThrow(() -> new IllegalStateException("Chunk should definitely exist after just updating it, what??"));
//		}
//	}

}
