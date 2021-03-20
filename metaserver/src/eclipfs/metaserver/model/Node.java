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
	protected final String location;
	protected final String name;

	private Node(final ResultSet result) throws SQLException {
		Validate.notNull(result);
		this.id = result.getLong("id");
		this.token = result.getString("token");
		this.location = result.getString("location");
		this.name = result.getString("name");
	}

	protected Node(final long id, final String token, final String location, final String name) {
		this.id = id;
		this.token = token;
		this.location = location;
		this.name = name;
	}

	public long getId() {
		return this.id;
	}

	public String getFullToken() {
		return this.token;
	}

	private String getReadToken() {
		final String readToken = this.token.substring(0, 64);
		return readToken;
	}

	private String getWriteToken() {
		final String writeToken = this.token.substring(64);
		return writeToken;
	}

	public String getToken(final TransferType transferType) {
		if (transferType == TransferType.UPLOAD) {
			return getWriteToken();
		} else if (transferType == TransferType.DOWNLOAD) {
			return getReadToken();
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
		final String token = RandomStringUtils.randomAlphanumeric(128);

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
