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

	private User(final ResultSet result) throws SQLException {
		this.id = result.getLong("id");
		this.username = result.getString("username");
		this.passwordHash = result.getString("password");
		this.writeAccess = result.getBoolean("write_access");
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
		Validate.notNull(this.passwordHash);

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

	public static User create(final String username, final String password) throws AlreadyExistsException, SQLException {
		Validate.notNull(username, "Username is null");
		Validate.notEmpty(password, "Password is null or empty");

		if (get(username).isPresent()) {
			throw new AlreadyExistsException("User already exists");
		}

		final String hash = MetaServer.PASSWORD_ENCODER.encode(password);

		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("INSERT INTO \"user\" (username, password) VALUES (?,?) RETURNING *")) {
				query.setString(1, username);
				query.setString(2, hash);
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
