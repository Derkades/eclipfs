package dsn_metaserver.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.Validate;

import dsn_metaserver.Database;

public class Node {
		
//	private static int lastUsedUploadIndex = 0;
//	private static int lastUsedDownloadIndex = 0;
	
	protected final long id;
	protected final String token;
	protected final float uptime;
	protected final float priorityDownload;
	protected final float priorityUpload;
	protected final String name;
	
	// Initialized by ping
	
	private Node(final ResultSet result) throws SQLException {
		Validate.notNull(result);
		this.id = result.getLong("id");
//		this.address = result.getString("address");
		this.token = result.getString("token");
		this.uptime = result.getFloat("uptime");
		this.priorityDownload = result.getFloat("priority_download");
		this.priorityUpload = result.getFloat("priority_upload");
		this.name = result.getString("name");
	}
	
	protected Node(final long id, final String token, final float uptime, final float priorityDownload, final float priorityUpload, final String name) {
		this.id = id;
		this.token = token;
		this.uptime = uptime;
		this.priorityDownload = priorityDownload;
		this.priorityUpload = priorityUpload;
		this.name = name;
	}
	
	public long getId() {
		return this.id;
	}
	
	public String getFullToken() {
		return this.token;
	}
	
	public String getReadToken() {
		final String readToken = this.token.substring(0, 64);
		Validate.isTrue(readToken.length() == 64); // TODO remove if it works
		return readToken;
	}
	
	public String getWriteToken() {
		final String writeToken = this.token.substring(64);
		Validate.isTrue(writeToken.length() == 64); // TODO remove if it works
		return writeToken;
	}
	
	public float getUptime() {
		return this.uptime;
	}
	
	public float getDownloadPriority() {
		return this.priorityDownload;
	}
	
	public float getUploadPriority() {
		return this.priorityUpload;
	}
	
	public String getName() {
		return this.name;
	}
	
	@Override
	public boolean equals(final Object other) {
		return other != null && other instanceof Node && ((Node) other).getId() == this.getId();
	}
	
	public static List<Node> listNodesDatabase() throws SQLException {
		final List<Node> nodes = new ArrayList<>();
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("SELECT * FROM \"node\"")) {
				final ResultSet result = query.executeQuery();
				while (result.next()) {
					nodes.add(new Node(result));
				}
			}
		}
		return nodes;
	}
	
//	public static Node getNodeForUpload() throws NodeShortageException {
//		synchronized(NODE_LOCK) {
//			if (FREE_SPACE_NODES.isEmpty()) {
//				throw new NodeShortageException();
//			}
//
//			if (lastUsedUploadIndex >= FREE_SPACE_NODES.size()) {
//				lastUsedUploadIndex = 0;
//			}
//
//			return FREE_SPACE_NODES.get(0);
//		}
//	}
//
//	public static Node getNodeForDownload() throws NodeShortageException {
//		synchronized(NODE_LOCK) {
//			if (ONLINE_NODES.isEmpty()) {
//				throw new NodeShortageException();
//			}
//
//			if (lastUsedDownloadIndex >= ONLINE_NODES.size()) {
//				lastUsedDownloadIndex = 0;
//			}
//
//			return ONLINE_NODES.get(0);
//		}
//	}
	
	public static Node createNode() throws SQLException {
		final String token = RandomStringUtils.randomAlphanumeric(128);
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("INSERT INTO \"node\" (token) VALUES (?) RETURNING *")) {
				query.setString(1, token);
				final ResultSet result = query.executeQuery();
				result.next();
				return new Node(result);
			}
		}
	}
	
	public static void deleteNode(final Node node) throws SQLException {
		Validate.notNull(node);
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("DELETE FROM \"node\" WHERE id=?")) {
				query.setLong(1, node.getId());
				query.execute();
			}
		}
		
		OnlineNode.removeNode(node);
	}

	public static Optional<Node> findByTokenInDatabase(final String token) throws SQLException {
		Validate.notNull(token);
		try (Connection connection = Database.getConnection()) {
			try (PreparedStatement query = connection.prepareStatement("SELECT * FROM \"node\" WHERE token=?")) {
				query.setString(1, token);
				final ResultSet result = query.executeQuery();
				if (result.next()) {
					return Optional.of(new Node(result));
				} else {
					return Optional.empty();
				}
			}
		}
	}

}
