package dsn_metaserver.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.Validate;

import dsn_metaserver.Database;
import dsn_metaserver.Validation;

public class File {
	
	private final long id;
	private final String name;
	private final long directoryId;
//	private final long size;
//	private final boolean distributed;
//	private boolean uploaded;
//	private final byte[] checksum;
//	private final int chunkSize;
	private final Long deleteTimestamp;
	
	private File(final ResultSet result) throws SQLException {
		this.id = result.getLong("id");
		this.name = result.getString("name");
		this.directoryId = result.getLong("directory");
//		this.size = result.getLong("size");
//		this.distributed = result.getBoolean("distributed");
//		this.uploaded = result.getBoolean("uploaded");
//		this.checksum = result.getBytes("checksum");
//		this.chunkSize = result.getInt("chunksize");
		this.deleteTimestamp = result.getObject("delete_time", Long.class); // Cannot use getLong, this is nullable
	}
	
	public long getId() {
		return this.id;
	}
	
	public String getName() {
		return this.name;
	}
	
	public Directory getDirectory() throws SQLException {
		return Directory.byId(this.directoryId).orElseThrow(() -> new IllegalStateException("Can't find directory with id " + this.directoryId + ". Something is VERY wrong."));
	}
	
	public long getDirectoryId() {
		return this.directoryId;
	}
	
	public long getSize() throws SQLException {
		try (final PreparedStatement query = Database.prepareStatement("SELECT SUM(size) FROM \"chunk\" WHERE file=?")) {
			query.setLong(1, this.getId());
			final ResultSet result = query.executeQuery();
			if (result.next()) {
				return result.getLong(1);
			} else {
				// No result likely means no chunks uploaded (yet).
				// it could also mean a severe bug, hopefully not.
				return 0;
			}
		}
	}
	
//	public long getCachedSize() {
//		return this.size;
//	}
	
//	public boolean isDistributed() {
//		return this.distributed;
//	}
	
//	public boolean isUploaded() {
//		synchronized(this) {
//			return this.uploaded;
//		}
//	}
	
//	public byte[] getChecksum() {
//		return this.checksum;
//	}
//
//	public String getChecksumHex() {
//		return Hex.encodeHexString(this.checksum);
//	}
	
//	public int getChunkSize() {
//		return this.chunkSize;
//	}
	
//	@Deprecated
//	public int calculateExpectedChunkCount() {
//		return (int) Math.ceil(((double) this.getSize()) / Tunables.CHUNKSIZE);
//	}
	
	public boolean isDeleted() {
		return this.deleteTimestamp != null;
	}
	
	public long getDeletedtimestamp() {
		return this.deleteTimestamp;
	}
	
//	public Chunk recreateChunk(final int index, final byte[] checksum) throws SQLException {
//		// some copy on write maybe even?
//		// - add chunk to second table
//		// - on upload complete, replace chunk in main table
//		synchronized(Chunk.class) {
//			Validate.isTrue(getChunk(index).isPresent(), "Cannot use recreateChunk if no chunk already exists, use createChunk instead.");
//			try (Connection connection = Database.getConnection()) {
//
//				// Delete any existing replacements
//				try (PreparedStatement query = connection.prepareStatement("DELETE FROM chunk_replacement WHERE file=? AND index=?")) {
//					query.setLong(1, this.getId());
//					query.setInt(2, index);
//				}
//
//				// Insert new replacement
//				try (PreparedStatement query = connection.prepareStatement("INSERT INTO chunk_replacement (index, file, checksum, token) VALUES (?,?,?,?) RETURNING *")) {
//					query.setInt(1, index);
//					query.setLong(2, this.id);
//					query.setBytes(3, checksum);
//					query.setString(4, RandomStringUtils.randomAlphanumeric(128));
//					final ResultSet result = query.executeQuery();
//					result.next();
//					return new Chunk(this, result);
//				}
//			}
//		}
//	}
	
	public Chunk createChunk(final int index, final byte[] checksum, final long size) throws SQLException {
		Validate.isTrue(index >= 0, "Chunk index must be positive");
		Validate.notNull(checksum);
		
		if (isDeleted()) {
			throw new IllegalStateException("Cannot upload new chunk for deleted file");
		}
		
		// Do not create multiple chunks at the same time
		synchronized(Chunk.class) {
			Validate.isTrue(getChunk(index).isEmpty(), "Chunk already exists");
			try (Connection connection = Database.getConnection()) {
				try (PreparedStatement query = connection.prepareStatement("INSERT INTO \"chunk\" (index, size, file, checksum, token) VALUES (?,?,?,?,?) RETURNING *")) {
					query.setInt(1, index);
					query.setLong(2, size);
					query.setLong(3, this.id);
					query.setBytes(4, checksum);
					query.setString(5, RandomStringUtils.randomAlphanumeric(128));
					final ResultSet result = query.executeQuery();
					result.next();
					return new Chunk(this, result);
				}
			}
		}
	}
	
	public Optional<Chunk> getChunk(final int index) throws SQLException {
		Validate.isTrue(index >= 0, "Chunk index must be positive");
		
		if (isDeleted()) {
			throw new IllegalStateException("Cannot download deleted file");
		}
		
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("SELECT * FROM \"chunk\" WHERE file=? AND index=?")) {
				query.setLong(1, this.getId());
				query.setInt(2, index);
				final ResultSet result = query.executeQuery();
				if (result.next()) {
					return Optional.of(new Chunk(this, result));
				} else {
					return Optional.empty();
				}
			}
		}
	}

//	public void deleteChunk(final int index) throws SQLException {
//		try (Connection connection = Database.getConnection()) {
//			try (PreparedStatement query = connection.prepareStatement("SELECT chunk_node.node, chunk.token FROM chunk_node JOIN chunk ON chunk_node.chunk = chunk.id WHERE chunk=?")) {
//				query.setLong(1, chunk.getId());
//				final ResultSet result = query.executeQuery();
//				while (result.next()) {
//					try (PreparedStatement createJob = connection.prepareStatement("INSERT INTO delete_jobs (\"node\", chunk_token) VALUES (?, ?)")){
//						createJob.setLong(1, result.getLong(1));
//						createJob.setString(2, result.getString(2));
//					}
//				}
//			}
//
//			try (PreparedStatement query = connection.prepareStatement("DELETE FROM \"chunk_node\" WHERE chunk=?")) {
//				query.setLong(1, chunk.getId());
//				query.execute();
//			}
//
//			try (PreparedStatement query = connection.prepareStatement("DELETE FROM \"chunk\" WHERE id=?")) {
//				query.setLong(1, chunk.getId());
//				query.execute();
//			}
//		}
//	}
	
//	public synchronized void upload(final byte[] fileContent) throws SQLException {
//		if (fileContent.length != this.getSize()) {
//			throw new IllegalArgumentException("fileContent.length does not match expected file size");
//		}
//
//		if (this.isUploaded()) {
//			throw new IllegalStateException("File is already uploaded");
//		}
//
//		try (Connection connection = Database.getConnection()) {
//			try (PreparedStatement query = connection.prepareStatement("INSERT INTO \"file_storage\" (\"file\", \"data\") VALUES (?, ?)")) {
//				query.setLong(1, this.id);
//				query.setBytes(2, fileContent);
//				query.execute();
//			}
//
//			try (PreparedStatement query = connection.prepareStatement("UPDATE \"file\" SET uploaded='True' WHERE id=?")) {
//				query.setLong(1, this.id);
//				query.execute();
//			}
//			this.uploaded = true;
//		}
//	}
	
	private static Optional<File> fileOptFromResult(final ResultSet result) throws SQLException {
		if (result.next()) {
			return Optional.of(new File(result));
		} else {
			return Optional.empty();
		}
	}
	
	public static Optional<File> byId(final long fileId) throws SQLException {
		Validate.isTrue(fileId >= 0);
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("SELECT * FROM \"file\" WHERE id=?")) {
				query.setLong(1, fileId);
				return fileOptFromResult(query.executeQuery());
			}
		}
	}
	
	static Optional<File> get(final long directoryId, final String name) throws SQLException {
		Validate.isTrue(directoryId >= 0);
		Validate.notEmpty(name);
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("SELECT * FROM \"file\" WHERE directory=? AND name=?")) {
				query.setLong(1, directoryId);
				query.setString(2, name);
				return fileOptFromResult(query.executeQuery());
			}
		}
	}
	
	/**
	 * 
	 * @param directoryId
	 * @return List of all files in a directory
	 * @throws SQLException
	 */
	static List<File> list(final long directoryId, final boolean deleted) throws SQLException {
		final String queryString = "SELECT * FROM \"file\" WHERE directory=? AND delete_time IS "
				+ (deleted ? "NOT NULL" : "NULL");
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement(queryString)) {
				query.setLong(1, directoryId);
				final ResultSet result = query.executeQuery();
				final List<File> files = new ArrayList<>();
				while (result.next()) {
					files.add(new File(result));
				}
				return files;
			}
		}
	}
	
	static File create(final long directoryId, final String name) throws SQLException {
		Validation.validateFileDirectoryName(name);
//		Validation.validateMD5(checksum);
//		final boolean distributed = fileSize < Tunables.DISTRIBUTE_THRESHOLD;
//		final int chunkSize = Tunables.CHUNKSIZE;
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("INSERT INTO \"file\" (name,directory) VALUES (?,?) RETURNING *")) {
				query.setString(1, name);
				query.setLong(2, directoryId);
//				query.setLong(3, fileSize);
//				query.setBoolean(4, distributed);
//				query.setBytes(5, checksum);
//				query.setInt(3, chunkSize);
				final ResultSet result = query.executeQuery();
				result.next();
				return new File(result);
			}
		}
	}
	
	@Override
	public boolean equals(final Object other) {
		return other != null && other instanceof File && ((File) other).getId() == this.getId();
	}
	
}
