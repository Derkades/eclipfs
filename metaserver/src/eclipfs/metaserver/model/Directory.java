package eclipfs.metaserver.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.Database;
import eclipfs.metaserver.Validation;
import eclipfs.metaserver.exception.AlreadyExistsException;

public class Directory extends Inode {

	protected Directory(final ResultSet result) throws SQLException {
		super(result);
	}

	@Override
	public boolean isFile() {
		return false;
	}

	@Override
	public long getSize() {
		return 0;
	}

	@Override
	public void delete() throws SQLException {
		if (!isEmpty()) {
			throw new UnsupportedOperationException("Cannot delete directory, it is not empty");
		}

		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("DELETE FROM \"inode\" WHERE id=?")) {
			query.setLong(1, this.getId());
			query.execute();
		}
	}

	public boolean isEmpty() throws SQLException {
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT id FROM inode WHERE parent=? LIMIT 1")){
			query.setLong(1, this.getId());
			return !query.executeQuery().next();
		}
	}

	public boolean contains(final String name) throws SQLException {
		Validation.validateFileDirectoryName(name);
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT id FROM inode WHERE parent=? AND name=?")) {
			query.setLong(1, this.getId());
			query.setString(2, name);
			return query.executeQuery().next();
		}
	}

	public Optional<Inode> getChild(final String name) throws SQLException{
		Validation.validateFileDirectoryName(name);
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT * FROM inode WHERE parent=? AND name=?")) {
			query.setLong(1, this.getId());
			query.setString(2, name);
			return optInodeFromResult(query.executeQuery());
		}
	}

	public List<Directory> listDirectories() throws SQLException {
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT * FROM inode WHERE parent=? AND is_file='False' AND id <> ?")) {
			query.setLong(1, this.getId());
			query.setLong(2, Inode.ROOT_INODE);
			final List<Directory> directories = new ArrayList<>();
			final ResultSet result = query.executeQuery();
			while (result.next()) {
				directories.add(new Directory(result));
			}
			return directories;
		}
	}

	public Directory createDirectory(final String name) throws AlreadyExistsException, SQLException {
		Validation.validateFileDirectoryName(name);
		if (this.contains(name)) {
			throw new AlreadyExistsException("A file or directory with the name " + name + " already exists");
		}

		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("INSERT INTO inode (name,parent,is_file,ctime,mtime) VALUES (?,?,'False',?,?) RETURNING *")) {
			query.setString(1, name);
			query.setLong(2, this.getId());
			query.setLong(3, System.currentTimeMillis());
			query.setLong(4, System.currentTimeMillis());
			final ResultSet result = query.executeQuery();
			result.next();
			return new Directory(result);
		}
	}

	public List<File> listFiles() throws SQLException {
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT * FROM inode WHERE parent=? AND is_file='True'")) {
			query.setLong(1, this.getId());
			final ResultSet result = query.executeQuery();
			final List<File> files = new ArrayList<>();
			while (result.next()) {
				files.add(new File(result));
			}
			return files;
		}
	}

	public File createFile(final String name) throws AlreadyExistsException, SQLException {
		Validation.validateFileDirectoryName(name);
		if (this.contains(name)) {
			throw new AlreadyExistsException("A file or directory with the name " + name + " already exists.");
		}

		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("INSERT INTO inode (name,parent,is_file,ctime,mtime) VALUES (?,?,'True',?,?) RETURNING *")) {
			query.setString(1, name);
			query.setLong(2, this.getId());
			query.setLong(3, System.currentTimeMillis());
			query.setLong(4, System.currentTimeMillis());
			final ResultSet result = query.executeQuery();
			result.next();
			return new File(result);
		}
	}

	public List<Inode> list() throws SQLException {
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT * FROM inode WHERE parent=? AND id <> ?")) {
			query.setLong(1, this.getId());
			query.setLong(2, Inode.ROOT_INODE);
			final ResultSet result = query.executeQuery();
			final List<Inode> children = new ArrayList<>();
			while (result.next()) {
				if (result.getBoolean("is_file")) {
					children.add(new File(result));
				} else {
					children.add(new Directory(result));
				}
			}
			return children;
		}
	}

	// Used by inodeInfo endpoint
	public void writeEntriesAsJsonDictionary(final JsonWriter writer) throws SQLException, IOException {
		writer.beginObject();
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT id,name FROM inode WHERE parent=? AND id <> ?")) {
			query.setLong(1, this.getId());
			query.setLong(2, Inode.ROOT_INODE);
			final ResultSet result = query.executeQuery();
			while (result.next()) {
				writer.name(result.getString("name")).value(result.getInt("id"));
			}
		}
		writer.endObject();
	}

}
