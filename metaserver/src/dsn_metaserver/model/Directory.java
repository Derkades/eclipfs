package dsn_metaserver.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import dsn_metaserver.Database;
import dsn_metaserver.Validation;
import dsn_metaserver.exception.AlreadyExistsException;
import dsn_metaserver.exception.NotDeletedException;
import dsn_metaserver.exception.NotEmptyException;
import dsn_metaserver.exception.NotExistsException;

public class Directory {

	private final long id;
	private String name;
	private transient Optional<Directory> parent;
	private final Long parentId;
//	private transient User owner;
//	private final long ownerId;
//	private final boolean publicRead;
//	private final boolean publicWrite;
	private Long deleteTime;
	
	private Directory(final ResultSet result) throws SQLException {
		this.id = result.getLong("id");
		this.name = result.getString("name");
		this.parentId = result.getObject("parent", Long.class);
//		this.ownerId = result.getLong("owner");
//		this.publicRead = result.getBoolean("public_read");
//		this.publicWrite = result.getBoolean("public_write");
		this.deleteTime = result.getObject("delete_time", Long.class);
	}
	
	public long getId() {
		return this.id;
	}

	public String getName() {
		return this.name;
	}
	
	public String getAboslutePath() throws SQLException {
		String fullPath = this.getName();
		Optional<Directory> optParent = this.getParent();
		while(optParent.isPresent()) {
			final Directory parent = optParent.get();
			fullPath = parent.getName() + "/" + fullPath;
			optParent = parent.getParent();
		}
		return "/" + fullPath;
	}
	
	public Optional<Directory> getParent() throws SQLException {
		if (this.parentId == null) {
			return Optional.empty();
		} else {
			if (this.parent == null) {
				this.parent = Optional.of(byId(this.parentId).get());
			}
			return this.parent;
		}
	}
	
	public Long getParentId() {
		return this.parentId;
	}
	
//	public User getOwner() throws SQLException {
//		if (this.owner == null) {
//			this.owner = User.get(this.ownerId).get();
//		}
//
//		return this.owner;
//	}
//
//	public long getOwnerId() {
//		return this.ownerId;
//	}
	
//	public boolean isPublicReadable() throws SQLException {
//		return this.publicRead;
//	}
//
//	public boolean isPublicWritable() throws SQLException {
//		return this.publicWrite;
//	}
	
	public boolean isDeleted() {
		return this.deleteTime != null;
	}
	
	public Date getDeleteTime() throws NotDeletedException {
		if (this.deleteTime == null) {
			throw new NotDeletedException("Cannot get delete time for file that is not deleted");
		}
		
		return new Date(this.deleteTime * 1000);
	}
	
	public File createFile(final String name) throws AlreadyExistsException, SQLException {
		checkNameNotExists(Optional.of(this), name);
		return File.create(this.id, name);
	}
	
	public List<Directory> listDirectories() throws SQLException {
		return Directory.listDirectories(Optional.of(this), false);
	}
	
	public List<Directory> listDeletedDirectories() throws SQLException {
		return Directory.listDirectories(Optional.of(this), true);
	}
	
	public List<File> listFiles() throws SQLException {
		return File.list(this.id, false);
	}
	
	public List<File> listDeletedFiles() throws SQLException {
		return File.list(this.id, true);
	}
	
	public Optional<File> getFile(final String fileName) throws SQLException {
		return File.get(this.id, fileName);
	}
	
	public Optional<Directory> getDirectory(final String subDirectoryName) throws SQLException {
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("SELECT * FROM directory WHERE parent=? AND name=? AND delete_time IS NULL")) {
				query.setLong(1, this.id);
				query.setString(2, subDirectoryName);
				return directoryOptionalFromResult(query.executeQuery());
			}
		}
	}
	
	public boolean containsDirectory(final String subDirectoryName) throws SQLException {
		Validation.validateFileDirectoryName(subDirectoryName);
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("SELECT id FROM directory WHERE parent=? AND name=? AND delete_time IS NULL")) {
				query.setLong(1, this.id);
				query.setString(2, subDirectoryName);
				return query.executeQuery().next();
			}
		}
	}
	
	public boolean containsFile(final String fileName) throws SQLException {
		Validation.validateFileDirectoryName(fileName);
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("SELECT id FROM file WHERE directory=? AND name=? AND delete_time IS NULL")) {
				query.setLong(1, this.id);
				query.setString(2, fileName);
				return query.executeQuery().next();
			}
		}
	}
	
	public void delete() throws NotEmptyException, SQLException {
		if (!listDirectories().isEmpty()) {
			throw new NotEmptyException("Directory can't be deleted because it contains subdirectories");
		}
		
		if (!listFiles().isEmpty()) {
			throw new NotEmptyException("Directory can't be deleted because it contains files");
		}
		
		if (this.isDeleted()) {
			throw new IllegalStateException("File is already deleted");
		}
		
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("UPDATE directory SET delete_time=? WHERE id=?")) {
				final long deleteTime = System.currentTimeMillis() / 1000;
				query.setLong(1, deleteTime);
				query.setLong(2, this.id);
				query.execute();
				this.deleteTime = deleteTime;
			}
		}
	}
	
	private static void checkNameNotExists(final Optional<Directory> optParent, final String name) throws AlreadyExistsException, SQLException {
		if (optParent.isPresent()) {
			final Directory parent = optParent.get();
			if (parent.containsDirectory(name)) {
				throw new AlreadyExistsException("A directory with the name " + name + " already exists");
			} else if (parent.containsFile(name)) {
				throw new AlreadyExistsException("A file with the name " + name + " already exists");
			}
		} else {
			if (Directory.getRootDirectoryByName(name).isPresent()) {
				throw new AlreadyExistsException("A directory with the name " + name + " already exists");
			}
		}
	}

	public void rename(final String newName) throws AlreadyExistsException, SQLException {
		Validation.validateFileDirectoryName(newName);
		checkNameNotExists(this.getParent(), newName);
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("UPDATE directory SET name=? WHERE id=?")) {
				query.setString(1, newName);
				query.setLong(2, this.getId());
				query.execute();
			}
		}
	}
	
	public void move(final String newFullPath) throws AlreadyExistsException, SQLException, NotExistsException {
		Validation.validatePath(newFullPath);
		final String newName = StringUtils.substringAfterLast(newFullPath, "/");
		final String newParentPath = StringUtils.substringBeforeLast(newFullPath, "/");
		if (newParentPath.isEmpty()) {
			// Moving to root directory
			move(Optional.empty(), newName);
		} else {
			// We cannot pass this optional directly to the other move method, otherwise
			// it would move the directory to the root directory instead of throwing an error!
			final Optional<Directory> optNewParent = Directory.findByPath(newParentPath);
			if (optNewParent.isEmpty()) {
				throw new NotExistsException("Target directory " + newParentPath + " does not exist");
			}
			move(optNewParent, newName);
		}
	}
	
	public void move(final Optional<Directory> newParent, final String newName) throws AlreadyExistsException, SQLException {
		Validation.validateFileDirectoryName(newName);
		checkNameNotExists(newParent, newName);
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("UPDATE directory SET name=?,parent=? WHERE id=?")) {
				query.setString(1, newName);
				if (newParent.isPresent()) {
					query.setLong(2, newParent.get().getId());
				} else {
					query.setNull(2, Types.BIGINT);
				}
				query.setLong(3, this.getId());
				query.execute();
			}
		}
		this.name = newName;
		this.parent = newParent;
	}
	
	public Directory createDirectory(final String name) throws AlreadyExistsException, SQLException {
		Validation.validateFileDirectoryName(name);
		checkNameNotExists(Optional.of(this), name);
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("INSERT INTO directory (name, parent) VALUES (?,?) RETURNING *")) {
				query.setString(1, name);
				query.setLong(2, this.getId());
				final ResultSet result = query.executeQuery();
				result.next();
				return new Directory(result);
			}
		}
	}
	
	public static Directory createRootDirectory(final String name) throws SQLException, AlreadyExistsException {
		Validation.validateFileDirectoryName(name);
		checkNameNotExists(Optional.empty(), name);
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("INSERT INTO directory (name) VALUES (?) RETURNING *")) {
				query.setString(1, name);
				final ResultSet result = query.executeQuery();
				result.next();
				return new Directory(result);
			}
		}
	}
	
//	public void move(final String newPath) throws AlreadyExistsException, SQLException, NotExistsException, IsDeletedException {
//		Validate.notBlank(newPath);
//		Validate.isTrue(!newPath.equals(this.path));
//
//		if (this.isDeleted()) {
//			throw new IsDeletedException("Cannot move deleted directory");
//		}
//
//		if (Directory.findByPath(newPath).isPresent()) {
//			throw new AlreadyExistsException("Directory already exists at new path");
//		}
//
//		final boolean moveToRoot = !newPath.contains("/");
//		Directory newParent = null;
//
//		if (moveToRoot) {
//
//		} else {
//			final String newDirName = StringUtils.substringAfterLast(newPath, "/");
//			final Optional<Directory> optNewParent = Directory.findByPath(StringUtils.substringBeforeLast(newPath, "/"));
//
//			if (optNewParent.isEmpty()) {
//				throw new NotExistsException("Target parent directory does not exist");
//			}
//
//			newParent = optNewParent.get();
//
//			if (newParent.getFile(newDirName).isPresent()) {
//				throw new AlreadyExistsException("A file with this name already exists");
//			}
//		}
//
//		final String queryString = "UPDATE directory SET path=?, parent=" +
//				(moveToRoot ? "NULL" : "?") +
//				" WHERE id=?";
//
//
//
//		try (Connection connection = Database.getConnection()) {
//			try (PreparedStatement query = connection.prepareStatement(queryString)) {
//				final long deleteTime = System.currentTimeMillis() / 1000;
//				query.setString(1, newPath);
//				if (!moveToRoot) {
//					Validate.notNull(newParent);
//					query.setLong(2, newParent.getId());
//				}
//				query.setLong(moveToRoot ? 2 : 3, this.id);
//				query.execute();
//				this.deleteTime = deleteTime;
//			}
//
//			// Move all subdirectories
//			move(connection, this.getId(), this.getPath(), newPath);
//		}
//
//		this.path = newPath;
//		if (moveToRoot) {
//			this.parent = Optional.empty();
//			this.parentId = null;
//		} else {
//			Validate.notNull(newParent);
//			this.parent = Optional.of(newParent);
//			this.parentId = newParent.getId();
//		}
//	}
//
//	private static void move(final Connection connection, final long topDirectoryId, final String pathFrom, final String pathTo) throws SQLException {
//		try (PreparedStatement getSubdirs = connection.prepareStatement("SELECT id,path FROM directory WHERE parent=?")) {
//			getSubdirs.setLong(1, topDirectoryId);
//			final ResultSet result = getSubdirs.executeQuery();
//			while (result.next()) {
//				final long subdirId = result.getLong("id");
//				final String subdirOldPath = result.getString("path");
//				final String subdirNewPath = StringUtils.replaceOnce(subdirOldPath, pathFrom, pathTo);
////				System.out.println("subdir id " + subdirId + " path " + subdirOldPath + " change to " + subdirNewPath + " pathFrom " + pathFrom + " pathTo " + pathTo);
//				try (PreparedStatement renamePath = connection.prepareStatement("UPDATE directory SET path=? WHERE id=?")){
//					renamePath.setString(1, subdirNewPath);
//					renamePath.setLong(2, subdirId);
//					renamePath.execute();
//				}
//				// Also update subdirectories of this subdirectory
//				move(connection, subdirId, pathFrom, pathTo);
//			}
//		}
//	}
	
	public static Optional<Directory> byId(final long id) throws SQLException {
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("SELECT * FROM directory WHERE id=?")) {
				query.setLong(1, id);
				return directoryOptionalFromResult(query.executeQuery());
			}
		}
	}
	
	public static Optional<Directory> getRootDirectoryByName(final String name) throws SQLException {
		Validation.validateFileDirectoryName(name);
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("SELECT * FROM directory WHERE name=? AND parent IS NULL AND delete_time IS NULL")) {
				query.setString(1, name);
				return directoryOptionalFromResult(query.executeQuery());
			}
		}
	}
	
	public static Optional<Directory> findByPath(final String path) throws SQLException {
		Validation.validatePath(path);
		final String[] pathComponents = path.split("/");
		// This should never happen. First component is always blank, second component has to exist
		Validate.isTrue(pathComponents.length > 1, "path split by / length too short, path: '" + path + "'");
		
		Optional<Directory> directory = getRootDirectoryByName(pathComponents[1]);
		if (pathComponents.length == 1) {
			return directory;
		}
		
		try (Connection connection = Database.getConnection()) {
			for (int i = 2; i < pathComponents.length; i++) {
				final String dirName = pathComponents[i];
				if (!directory.isPresent()) {
					return Optional.empty();
				}
				
				directory = directory.get().getDirectory(dirName);
			}
		}
		
		return directory;
	}
	
	
//	public static Optional<Directory> findByPath(final String path) throws SQLException {
//		return findByPath(path, false);
//	}
//
//	public static Optional<Directory> findByPath(final String path, final boolean deleted) throws SQLException {
//		Validation.validatePath(path);
//
//		try (Connection connection = Database.getConnection()) {
//			final String queryString = "SELECT * FROM directory WHERE path=? AND delete_time IS " +
//					(deleted ? "NOT NULL" : "NULL");
//			try (PreparedStatement query = connection.prepareStatement(queryString)) {
//				query.setString(1, path);
//				return directoryOptionalFromResult(query.executeQuery());
//			}
//		}
//	}
	
	private static Optional<Directory> directoryOptionalFromResult(final ResultSet result) throws SQLException {
		if (result.next()) {
			return Optional.of(new Directory(result));
		} else {
			return Optional.empty();
		}
	}
	
//	public static Directory createDirectory(final Optional<Directory> parent, final String name) throws SQLException, AlreadyExistsException {
//		Validate.notNull(parent);
//		Validation.validateFileDirectoryName(name);
//
//		final Long parentId;
//
//		MetaServer.LOGGER.info("Creating directory with name " + name);
//
//		if (findByPath(path).isPresent()) {
//			throw new AlreadyExistsException("Directory with this path already exists");
//		}
//
//		Validation.validatePath(path);
//
//		try (Connection connection = Database.getConnection()) {
//			final String queryString;
////			if (parent.isPresent()) {
////				queryString = "INSERT INTO directory (path,parent,owner,public_read,public_write) VALUES (?,?,?,?,?,?)";
////			} else {
//				queryString = "INSERT INTO directory (path,parent) VALUES (?,?)";
////			}
//			try (PreparedStatement query = connection.prepareStatement(queryString)) {
//				query.setString(1, path);
//				query.setObject(2, parentId, Types.BIGINT);
////				if (parent.isPresent()) {
////					query.setBoolean(4, parent.get().isPublicReadable());
////					query.setBoolean(5, parent.get().isPublicWritable());
////				}
//				query.execute();
//			}
//		}
//
//		return findByPath(path).get();
//	}
	
	public static List<Directory> getRootDirectories() throws SQLException{
		return listDirectories(Optional.empty(), false);
	}
	
	public static List<Directory> getDeletedRootDirectories() throws SQLException {
		return listDirectories(Optional.empty(), true);
	}
	
	private static List<Directory> listDirectories(final Optional<Directory> directory, final boolean deleted) throws SQLException {
		Validate.notNull(directory);
		
		final List<Directory> directories = new ArrayList<>();
		
		try (Connection connection = Database.getConnection()) {
			final String queryString = "SELECT * FROM directory WHERE parent"
					+ (directory.isPresent() ? "=?" : " IS NULL")
					+ " AND delete_time IS "
					+ (deleted ? "NOT NULL" : "NULL");
			try (PreparedStatement query = connection.prepareStatement(queryString)) {
				if (directory.isPresent()) {
					query.setObject(1, directory.get().getId(), Types.BIGINT);
				}
				final ResultSet result = query.executeQuery();
				while (result.next()) {
					directories.add(new Directory(result));
				}
			}
		}
		
		return directories;
	}

}
