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
import eclipfs.metaserver.MetaServer;
import eclipfs.metaserver.Validation;
import eclipfs.metaserver.exception.AlreadyExistsException;

public class User {

	private final long id;
	private final String username;
	private final String passwordHash;
	private boolean writeAccess;
//	private final Long quotaCount;
//	private final Long quotaSize;
	
	private User(final ResultSet result) throws SQLException {
		this.id = result.getLong("id");
		this.username = result.getString("username");
		this.passwordHash = result.getString("password");
		this.writeAccess = result.getBoolean("write_access");
//		this.quotaCount = result.getObject("quota_count", Long.class);
//		this.quotaSize = result.getObject("quota_count", Long.class);
	}
	
	public long getId() {
		return this.id;
	}
	
	public String getUsername() {
		return this.username;
	}
	
	public boolean hasPassword() {
		return this.passwordHash != null;
	}
	
	public boolean verifyPassword(final String password) {
		Validate.notNull(password);
		
		if (this.passwordHash == null) {
			return true;
		}
		
        return MetaServer.PASSWORD_ENCODER.matches(password, this.passwordHash);
	}
	
	public boolean hasWriteAccess() {
		return this.writeAccess;
	}
	
	public void setWriteAccess(final boolean hasWriteAccess) throws SQLException {
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("UPDATE \"user\" SET write_access=? WHERE id=?")) {
				query.setBoolean(1, hasWriteAccess);
				query.setLong(2, this.getId());
				query.execute();
				this.writeAccess = hasWriteAccess;
			}
		}
	}
	
//	public Optional<Long> getCountQuota() {
//		return Optional.ofNullable(this.quotaCount);
//	}
//
//	public Optional<Long> getSizeQuota() {
//		return Optional.ofNullable(this.quotaSize);
//	}
	
//	public boolean hasReadAccess(final Directory directory) throws SQLException {
//		return checkPermission("read", directory.getId());
//	}
//
//	public boolean hasWriteAccess(final Directory directory) throws SQLException {
//		return checkPermission("write", directory.getId());
//	}
	
//	private boolean checkPermission(final String permissionType, final long directoryId) throws SQLException {
//		Validate.isTrue(permissionType.equals("read") || permissionType.equals("write"));
//		final String queryString = String.format("SELECT owner,public_%s FROM directory WHERE id=?", permissionType, permissionType);
//
//		try (Connection connection = Database.getConnection()) {
//			try (PreparedStatement query = connection.prepareStatement(queryString)) {
//				query.setLong(1, directoryId);
//				final ResultSet result = query.executeQuery();
//				if (!result.next()) {
//					throw new IllegalArgumentException("Directory does not exist?");
//				}
//				final long ownerId = result.getLong("owner");
//
//				if (this.getId() == ownerId) {
//					// User owns this file
//					return true;
//				}
//
//				return result.getBoolean("public_" + permissionType);
//			}
//		}
//	}
	
//	public boolean hasReadAccess(final File file) throws SQLException {
//		final Optional<Directory> optDir = file.getDirectory();
//		if (!optDir.isEmpty()) {
//			throw new IllegalArgumentException("File has no parent directory. That should not even be possible?");
//		}
//
//		return hasReadAccess(optDir.get());
//	}
//
//	public boolean hasWriteAccess(final File file) throws SQLException {
//		final Optional<Directory> optDir = file.getDirectory();
//		if (!optDir.isEmpty()) {
//			throw new IllegalArgumentException("File has no parent directory. That should not even be possible?");
//		}
//
//		return hasWriteAccess(optDir.get());
//	}
	
	public static Optional<User> get(final long id) throws SQLException {
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("SELECT * FROM \"user\" WHERE id=?")) {
				query.setLong(1, id);
				return resultToOptionalUser(query.executeQuery());
			}
		}
	}
	
	public static Optional<User> get(final String username) throws SQLException {
		Validation.validateUsername(username);
		
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("SELECT * FROM \"user\" WHERE username=?")) {
				query.setString(1, username);
				return resultToOptionalUser(query.executeQuery());
			}
		}
	}
	
	public static User create(final String username) throws AlreadyExistsException, SQLException {
		Validate.notNull(username);
		
		if (get(username).isPresent()) {
			throw new AlreadyExistsException("User already exists");
		}
		
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("INSERT INTO \"user\" (username) VALUES (?) RETURNING *")) {
				query.setString(1, username);
				final ResultSet result = query.executeQuery();
				result.next();
				return new User(result);
			}
		}
	}
	
	public static List<User> list() throws SQLException {
		final List<User> users = new ArrayList<>();
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("SELECT * FROM \"user\"")) {
				final ResultSet result = query.executeQuery();
				while (result.next()) {
					users.add(new User(result));
				}
			}
		}
		return users;
	}
	
	private static Optional<User> resultToOptionalUser(final ResultSet result) throws SQLException {
		Validate.notNull(result);
		if (result.next()) {
			return Optional.of(new User(result));
		} else {
			return Optional.empty();
		}
	}
	
	@Override
	public String toString() {
		return this.getUsername() + "[" + this.getId() + "]";
	}

}
