package eclipfs.metaserver.model;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.Validate;

import eclipfs.metaserver.Database;

public class File extends Inode {
	
	protected File(final ResultSet result) throws SQLException {
		super(result);
	}
	
	@Override
	public boolean isFile() {
		return true;
	}
	
	@Override
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
	
	@Override
	public void delete() throws SQLException {
		try (final PreparedStatement query = Database.prepareStatement("DELETE FROM inode WHERE id=?")) {
			query.setLong(1, this.getId());
			query.execute();
		}
	}
	
//	public boolean isDeleted() {
//		return this.deleteTimestamp != null;
//	}
//
//	public long getDeletedtimestamp() {
//		return this.deleteTimestamp;
//	}
	
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
		
		// Do not create multiple chunks at the same time
		synchronized(Chunk.class) {
			Validate.isTrue(getChunk(index).isEmpty(), "Chunk already exists");
			try (PreparedStatement query = Database.prepareStatement("INSERT INTO \"chunk\" (index, size, file, checksum, token) VALUES (?,?,?,?,?) RETURNING *")) {
				query.setInt(1, index);
				query.setLong(2, size);
				query.setLong(3, this.getId());
				query.setBytes(4, checksum);
				query.setString(5, RandomStringUtils.randomAlphanumeric(128));
				final ResultSet result = query.executeQuery();
				result.next();
				return new Chunk(this, result);
			}
		}
	}
	
	public Optional<Chunk> getChunk(final int index) throws SQLException {
		Validate.isTrue(index >= 0, "Chunk index must be positive");
		
		try (PreparedStatement query = Database.prepareStatement("SELECT * FROM \"chunk\" WHERE file=? AND index=?")) {
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
	
	public int getMaxChunkIndex() throws SQLException {
		try (PreparedStatement query = Database.prepareStatement("SELECT MAX(index) FROM \"chunk\" WHERE file=?")) {
			query.setLong(1, this.getId());
			final ResultSet result = query.executeQuery();
			result.next();
			return result.getInt(1);
		}
	}

	public List<Long> listChunkIds() throws SQLException {
		try (PreparedStatement query = Database.prepareStatement("SELECT id FROM \"chunk\" WHERE file=?")) {
			query.setLong(1, this.getId());
			final ResultSet result = query.executeQuery();
			final List<Long> ids = new ArrayList<>();
			while (result.next()) {
				ids.add(result.getLong(1));
			}
			return ids;
		}
	}
	
//	public static Optional<File> byId(final long fileId) throws SQLException {
//		Validate.isTrue(fileId >= 0);
//		try (PreparedStatement query = Database.prepareStatement("SELECT * FROM \"file\" WHERE id=?")) {
//			query.setLong(1, fileId);
//			return fileOptFromResult(query.executeQuery());
//		}
//	}
//
//	public static Optional<File> byInode(final long fileId) throws SQLException {
//		Validate.isTrue(fileId >= 0);
//		try (PreparedStatement query = Database.prepareStatement("SELECT * FROM \"file\" WHERE inode=?")) {
//			query.setLong(1, fileId);
//			return fileOptFromResult(query.executeQuery());
//		}
//	}
	
//	static Optional<File> get(final long directoryId, final String name) throws SQLException {
//		Validate.isTrue(directoryId >= 0);
//		Validate.notEmpty(name);
//		try (PreparedStatement query = Database.prepareStatement("SELECT * FROM \"file\" WHERE directory=? AND name=?")) {
//			query.setLong(1, directoryId);
//			query.setString(2, name);
//			return fileOptFromResult(query.executeQuery());
//		}
//	}
	
//	/**
//	 *
//	 * @param directoryId
//	 * @return List of all files in a directory
//	 * @throws SQLException
//	 */
//	static List<File> list(final long directoryId, final boolean deleted) throws SQLException {
//		final String queryString = "SELECT * FROM \"file\" WHERE directory=? AND delete_time IS "
//				+ (deleted ? "NOT NULL" : "NULL");
//		try (PreparedStatement query = Database.prepareStatement(queryString)) {
//			query.setLong(1, directoryId);
//			final ResultSet result = query.executeQuery();
//			final List<File> files = new ArrayList<>();
//			while (result.next()) {
//				files.add(new File(result));
//			}
//			return files;
//		}
//	}
	
//	static File create(final long directoryId, final String name) throws SQLException {
//		Validation.validateFileDirectoryName(name);
//		try (PreparedStatement query = Database.prepareStatement("INSERT INTO \"file\" (name,directory) VALUES (?,?) RETURNING *")) {
//			query.setString(1, name);
//			query.setLong(2, directoryId);
//			final ResultSet result = query.executeQuery();
//			result.next();
//			return new File(result);
//		}
//	}

}
