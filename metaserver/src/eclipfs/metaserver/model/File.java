package eclipfs.metaserver.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
		try (Connection conn = Database.getConnection();
				final PreparedStatement query = conn.prepareStatement("SELECT SUM(size) FROM \"chunk\" WHERE file=?")) {
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
		try (Connection conn = Database.getConnection();
				final PreparedStatement query = conn.prepareStatement("DELETE FROM inode WHERE id=?")) {
			query.setLong(1, this.getId());
			query.execute();
		}
	}

	public WritingChunk createChunk(final int index, final byte[] checksum, final long size) throws SQLException {
		Validate.isTrue(index >= 0, "Chunk index must be positive");
		Validate.notNull(checksum);

		// Do not create multiple chunks at the same time
		synchronized(Chunk.class) {
//			Validate.isTrue(getChunk(index).isEmpty(), "Chunk already exists");
			try (Connection conn = Database.getConnection();
					PreparedStatement query = conn.prepareStatement(
							"INSERT INTO \"chunk_writing\""
							+ "(file, index, size, checksum, time) VALUES (?,?,?,?,?) "
							+ "ON CONFLICT(file, index) DO UPDATE SET time=? "
							+ "RETURNING *")) {
				query.setLong(1, this.getId());
				query.setInt(2, index);
				query.setLong(3, size);
				query.setBytes(4, checksum);
				query.setLong(5, System.currentTimeMillis() / 1000);
				query.setLong(6, System.currentTimeMillis() / 1000);
//				query.setString(5, RandomStringUtils.randomAlphanumeric(128));
				final ResultSet result = query.executeQuery();
				result.next();
				return new WritingChunk(this, result);
			}
		}
	}

	public Optional<Chunk> getChunk(final int index) throws SQLException {
		Validate.isTrue(index >= 0, "Chunk index must be positive");

		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT * FROM \"chunk\" WHERE file=? AND index=?")) {
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

	public void deleteChunk(final int index) throws SQLException {
		Validate.isTrue(index >= 0, "Chunk index must be positive");

		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("DELETE FROM \"chunk\" WHERE file=? AND index=?")) {
			query.setLong(1, this.getId());
			query.setInt(2, index);
			query.execute();
		}
	}

	public boolean hasChunk(final int index) throws SQLException {
		Validate.isTrue(index >= 0, "Chunk index must be positive");

		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT id FROM \"chunk\" WHERE file=? AND index=?")) {
			query.setLong(1, this.getId());
			query.setInt(2, index);
			return query.executeQuery().next();
		}
	}

	public int getMaxChunkIndex() throws SQLException {
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT MAX(index) FROM \"chunk\" WHERE file=?")) {
			query.setLong(1, this.getId());
			final ResultSet result = query.executeQuery();
			result.next();
			return result.getInt(1);
		}
	}

	public List<Long> listChunkIds() throws SQLException {
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT id FROM \"chunk\" WHERE file=?")) {
			query.setLong(1, this.getId());
			final ResultSet result = query.executeQuery();
			final List<Long> ids = new ArrayList<>();
			while (result.next()) {
				ids.add(result.getLong(1));
			}
			return ids;
		}
	}

}
