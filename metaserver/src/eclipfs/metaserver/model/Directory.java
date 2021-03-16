package eclipfs.metaserver.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import eclipfs.metaserver.Database;
import eclipfs.metaserver.Validation;
import eclipfs.metaserver.exception.AlreadyExistsException;

public class Directory extends Inode {

//	private final long id;
//	private String name;
//	private transient Optional<Directory> parent;
//	private final Long parentId;
////	private transient User owner;
////	private final long ownerId;
////	private final boolean publicRead;
////	private final boolean publicWrite;
//	private Long deleteTime;
//
//	Directory(final ResultSet result) throws SQLException {
//		this.id = result.getLong("id");
//		this.name = result.getString("name");
//		this.parentId = result.getObject("parent", Long.class);
////		this.ownerId = result.getLong("owner");
////		this.publicRead = result.getBoolean("public_read");
////		this.publicWrite = result.getBoolean("public_write");
//		this.deleteTime = result.getObject("delete_time", Long.class);
//	}
//
//	@Override
//	public long getId() {
//		return this.id;
//	}
//
//	@Override
//	public String getName() {
//		return this.name;
//	}
	
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

//	private static Optional<Directory> directoryOptionalFromResult(final ResultSet result) throws SQLException {
//		if (result.next()) {
//			return Optional.of(new Directory(result));
//		} else {
//			return Optional.empty();
//		}
//	}
//
//	private static Optional<File> fileOptFromResult(final ResultSet result) throws SQLException {
//		if (result.next()) {
//			return Optional.of(new File(result));
//		} else {
//			return Optional.empty();
//		}
//	}
	
//	private static void checkNameNotExists(Directory parent, final String name) throws AlreadyExistsException, SQLException {
//		if (optParent.isPresent()) {
//			final Directory parent = optParent.get();
//			if (parent.containsDirectory(name)) {
//				throw new AlreadyExistsException("A directory with the name " + name + " already exists");
//			} else if (parent.containsFile(name)) {
//				throw new AlreadyExistsException("A file with the name " + name + " already exists");
//			}
//		} else {
//			if (Directory.getRootDirectoryByName(name).isPresent()) {
//				throw new AlreadyExistsException("A directory with the name " + name + " already exists");
//			}
//		}
//	}
	
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
	
//	public Optional<Directory> getDirectory(final String name) throws SQLException {
//		Validation.validateFileDirectoryName(name);
//		try (PreparedStatement query = Database.prepareStatement("SELECT * FROM \"inode\" WHERE parent=? AND name=? AND is_file='False'")) {
//			query.setLong(1, this.getId());
//			query.setString(2, name);
//			return directoryOptionalFromResult(query.executeQuery());
//		}
//	}
	
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
	
//	public Optional<File> getFile(final String fileName) throws SQLException {
//		Validation.validateFileDirectoryName(fileName);
//		try (PreparedStatement query = Database.prepareStatement("SELECT * FROM \"inode\" WHERE \"parent\"=? AND name=?")) {
//			query.setLong(1, this.getId());
//			query.setString(2, fileName);
//			return fileOptFromResult(query.executeQuery());
//		}
//	}
	
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

//	public void rename(final String newName) throws AlreadyExistsException, SQLException {
//		Validation.validateFileDirectoryName(newName);
//		if (this.contains(name)) {
//			throw new AlreadyExistsException();
//		}
//		try (Connection connection = Database.getConnection()) {
//			try (PreparedStatement query = connection.prepareStatement("UPDATE directory SET name=? WHERE id=?")) {
//				query.setString(1, newName);
//				query.setLong(2, this.getId());
//				query.execute();
//			}
//		}
//	}
	
//	public void move(final String newFullPath) throws AlreadyExistsException, SQLException, NotExistsException {
//		Validation.validatePath(newFullPath);
//		final String newName = StringUtils.substringAfterLast(newFullPath, "/");
//		final String newParentPath = StringUtils.substringBeforeLast(newFullPath, "/");
//		if (newParentPath.isEmpty()) {
//			// Moving to root directory
//			move(Optional.empty(), newName);
//		} else {
//			// We cannot pass this optional directly to the other move method, otherwise
//			// it would move the directory to the root directory instead of throwing an error!
//			final Optional<Directory> optNewParent = Directory.findByPath(newParentPath);
//			if (optNewParent.isEmpty()) {
//				throw new NotExistsException("Target directory " + newParentPath + " does not exist");
//			}
//			move(optNewParent, newName);
//		}
//	}
//
//	public void move(final Optional<Directory> newParent, final String newName) throws AlreadyExistsException, SQLException {
//		Validation.validateFileDirectoryName(newName);
//		checkNameNotExists(newParent, newName);
//		try (Connection connection = Database.getConnection()) {
//			try (PreparedStatement query = connection.prepareStatement("UPDATE directory SET name=?,parent=? WHERE id=?")) {
//				query.setString(1, newName);
//				if (newParent.isPresent()) {
//					query.setLong(2, newParent.get().getId());
//				} else {
//					query.setNull(2, Types.BIGINT);
//				}
//				query.setLong(3, this.getId());
//				query.execute();
//			}
//		}
//		this.name = newName;
//		this.parent = newParent;
//	}
	
//	public static Directory createRootDirectory(final String name) throws SQLException, AlreadyExistsException {
//		Validation.validateFileDirectoryName(name);
//		checkNameNotExists(Optional.empty(), name);
//		try (Connection connection = Database.getConnection()) {
//			try (PreparedStatement query = connection.prepareStatement("INSERT INTO directory (name) VALUES (?) RETURNING *")) {
//				query.setString(1, name);
//				final ResultSet result = query.executeQuery();
//				result.next();
//				return new Directory(result);
//			}
//		}
//	}
	
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
	
//	public static Optional<Directory> findByPath(final String path) throws SQLException {
//		Validation.validatePath(path);
//		final String[] pathComponents = path.split("/");
//		// This should never happen. First component is always blank, second component has to exist
//		Validate.isTrue(pathComponents.length > 1, "path split by / length too short, path: '" + path + "'");
//
//		Optional<Directory> directory = getRootDirectoryByName(pathComponents[1]);
//		if (pathComponents.length == 1) {
//			return directory;
//		}
//
//		try (Connection connection = Database.getConnection()) {
//			for (int i = 2; i < pathComponents.length; i++) {
//				final String dirName = pathComponents[i];
//				if (!directory.isPresent()) {
//					return Optional.empty();
//				}
//
//				directory = directory.get().getDirectory(dirName);
//			}
//		}
//
//		return directory;
//	}
	
	
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
	
//	public static List<Directory> getRootDirectories() throws SQLException{
//		return listDirectories(Optional.empty(), false);
//	}
//
//	public static List<Directory> getDeletedRootDirectories() throws SQLException {
//		return listDirectories(Optional.empty(), true);
//	}

}
