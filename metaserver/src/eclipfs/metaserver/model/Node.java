package eclipfs.metaserver.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.Validate;

import eclipfs.metaserver.Database;
import eclipfs.metaserver.TransferType;

public class Node {

	protected final long id;
	protected final String token;
	protected final String name;
	protected final String location;

	private Node(final ResultSet result) throws SQLException {
		Validate.notNull(result, "result is null");
		this.id = result.getLong("id");
		this.token = result.getString("token");
		this.name = result.getString("name");
		this.location = result.getString("location");
	}

	protected Node(final long id, final String token, final String location, final String name) {
		Validate.isTrue(id >= 0, "Id is negative");
		Validate.notNull(token, "Token is null");
		Validate.notNull(location, "Location is null");
		Validate.notNull(name, "Name is null");
		this.id = id;
		this.token = token;
		this.name = name;
		this.location = location;
	}

	public long getId() {
		return this.id;
	}

	public String getToken() {
		return this.token;
	}

	public String getReadOnlyToken() {
		return this.token.substring(0, 16);
	}

	public String getToken(final TransferType transferType) {
		Validate.notNull(transferType, "Transfer type is null");
		if (transferType == TransferType.UPLOAD) {
			return getToken();
		} else if (transferType == TransferType.DOWNLOAD) {
			return getReadOnlyToken();
		} else {
			throw new IllegalStateException(transferType.name());
		}
	}

	public String getName() {
		return this.name;
	}

	public String getLocation() {
		return this.location;
	}

	public int getStoredChunkCount() throws SQLException {
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT COUNT(*) FROM chunk_node WHERE node=?")) {
			query.setLong(1, this.getId());
			final ResultSet result = query.executeQuery();
			result.next();
			return result.getInt(1);
		}
	}

	public long getStoredChunkSize() throws SQLException {
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT SUM(size) FROM chunk JOIN chunk_node ON chunk.id=chunk WHERE node=?")) {
			query.setLong(1, this.getId());
			final ResultSet result = query.executeQuery();
			result.next();
			return result.getLong(1);
		}
	}

	public boolean hasChunk(final long chunkId) throws SQLException {
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT * FROM \"chunk_node\" WHERE chunk=? AND node=?")) {
			query.setLong(1, chunkId);
			query.setLong(2, this.getId());
			return query.executeQuery().next();
		}
	}

	@Override
	public boolean equals(final Object other) {
		return other != null && other instanceof Node && ((Node) other).getId() == this.getId();
	}

	public static List<Node> listNodesDatabase() throws SQLException {
		final List<Node> nodes = new ArrayList<>();
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT id FROM \"node\"")) {
			final ResultSet result = query.executeQuery();
			while (result.next()) {
				final long id = result.getLong(1);
				nodes.add(Node.byId(id).orElseThrow(() -> new IllegalStateException("Impossible, node must exist, it was just selected from the database")));
			}
		}
		return Collections.unmodifiableList(nodes);
	}

	public static Node createNode(final String name, final String location) throws SQLException {
		Validate.notNull(name, "Name is null");
		Validate.notNull(location, "Location is null");

//		final String token = RandomStringUtils.randomAlphanumeric(128);
		final String token = RandomStringUtils.randomAlphanumeric(32);

		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("INSERT INTO \"node\" (token, name, location) VALUES (?, ?, ?) RETURNING *")) {
			query.setString(1, token);
			query.setString(2, name);
			query.setString(3, location);
			final ResultSet result = query.executeQuery();
			result.next();
			return new Node(result);
		}
	}

	public static void deleteNode(final Node node) throws SQLException {
		Validate.notNull(node);

		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("DELETE FROM \"node\" WHERE id=?")) {
			query.setLong(1, node.getId());
			query.execute();
		}
		OnlineNode.removeNode(node);
	}

	private static Optional<Node> resultToOptNode(final ResultSet result) throws SQLException {
		if (result.next()) {
			return Optional.of(new Node(result));
		} else {
			return Optional.empty();
		}
	}

	public static Optional<Node> byToken(final String token) throws SQLException {
		Validate.notNull(token);
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT * FROM \"node\" WHERE token=?")) {
			query.setString(1, token);
			return resultToOptNode(query.executeQuery());
		}
	}

	public static Optional<Node> byId(final long id) throws SQLException {
		final Optional<OnlineNode> optOnline = OnlineNode.getOnlineNodeById(id);
		if (optOnline.isPresent()) {
			return Optional.of(optOnline.get());
		}
		try (Connection conn = Database.getConnection();
				PreparedStatement query = conn.prepareStatement("SELECT * FROM \"node\" WHERE id=?")) {
			query.setLong(1, id);
			return resultToOptNode(query.executeQuery());
		}
	}

}
