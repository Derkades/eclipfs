package eclipfs.metaserver.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import org.apache.commons.lang3.Validate;
import org.springframework.security.crypto.codec.Hex;

import eclipfs.metaserver.Database;

public class WritingChunk {

	private final long id;
	private final int index;
	private final byte[] checksum;

	private final File file;

	WritingChunk(final File file, final ResultSet result) throws SQLException {
		Validate.notNull(file, "File is null");
		Validate.notNull(result, "ResultSet is null");
		this.id = result.getLong("id");
		this.index = result.getInt("index");
		Validate.isTrue(result.getLong("file") == file.getId());
		this.checksum = result.getBytes("checksum");
//		this.token = result.getString("token");
		this.file = file;
	}

	public long getId() {
		return this.id;
	}

	public int getIndex() {
		return this.index;
	}

	public File getFile() {
		return this.file;
	}

	public byte[] getChecksum() {
		return this.checksum;
	}

	public String getChecksumHex() {
		return new String(Hex.encode(this.getChecksum()));
	}

	public Chunk finalizeChunk() throws SQLException {
		// Do not create multiple chunks at the same time
		synchronized(Chunk.class) {
			try (Connection conn = Database.getConnection()) {
				try (PreparedStatement query = conn.prepareStatement("DELETE FROM chunk_writing WHERE id=?")) {
					query.setLong(1, this.getId());
					query.execute();
				}
				try (PreparedStatement query = conn.prepareStatement("INSERT INTO \"chunk\" (file, index, checksum) VALUES (?,?,?) RETURNING *")) {
					query.setLong(1, this.getFile().getId());
					query.setInt(2, this.getIndex());
					query.setBytes(3, this.getChecksum());
					final ResultSet result = query.executeQuery();
					result.next();
					return new Chunk(this.getFile(), result);
				}
			}
		}
	}

	public synchronized static Optional<WritingChunk> byId(final long id) throws SQLException {
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT * FROM \"chunk_writing\" WHERE id=?")) {
			query.setLong(1, id);
			final ResultSet result = query.executeQuery();
			if (result.next()) {
				final long fileId = result.getLong("file");
				final Optional<Inode> optFile = Inode.byId(fileId);
				if (optFile.isEmpty()) {
					throw new IllegalStateException("Orphan chunk: file no longer exists. File id: " + fileId);
				}
				return Optional.of(new WritingChunk((File) optFile.get(), result));
			} else {
				return Optional.empty();
			}
		}
	}

	public static boolean exists(final long id) throws SQLException {
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT * FROM \"chunk_writing\" WHERE id=?")) {
			query.setLong(1, id);
			return query.executeQuery().next();
		}
	}

}
