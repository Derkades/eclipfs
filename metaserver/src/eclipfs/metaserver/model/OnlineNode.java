package eclipfs.metaserver.model;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.Validate;

import eclipfs.metaserver.TransferType;
import eclipfs.metaserver.Tunables;
import eclipfs.metaserver.Validation;
import eclipfs.metaserver.exception.NodeNotFoundException;

public class OnlineNode extends Node {
	
	private static final Object NODE_LOCK = new Object();
//	private static final Set<OnlineNode> FREE_SPACE_NODES = new HashSet<>();
	private static final List<OnlineNode> ONLINE_NODES = new ArrayList<>();
	private static final Map<Long, OnlineNode> BY_ID = new HashMap<>();
	private static final Map<String, OnlineNode> BY_TOKEN = new HashMap<>();
	
	private long freeSpace = -1;
	private long lastAnnounce = -1;
	private final String version = null;
	private URL address;
	private String label;
	
	public OnlineNode(final Node node) {
		super(node.id, node.token, node.uptime, node.priorityDownload, node.priorityUpload, node.name);
	}

	public long getFreeSpace() {
		if (this.freeSpace == -1) {
			throw new IllegalStateException();
		}
		return this.freeSpace;
	}
	
	public long getLastAnnounce() {
		if (this.lastAnnounce == -1) {
			throw new IllegalStateException();
		}
		return this.lastAnnounce;
	}

	public String getVersion() {
		if (this.version == null) {
			throw new IllegalStateException();
		}
		return this.version;
	}
	
	private void updateAddress(final URL url) throws SQLException {
		Validate.notNull(url);
		this.address = url;
	}
	
	public URL getAddress() {
		Validate.notNull(this.address);
		return this.address;
	}
	
	public String getLabel() {
		Validate.notNull(this.label);
		return this.label;
	}
	
	public boolean requestReplicate(final Chunk chunk, final OnlineNode target) throws IOException {
		final String targetAddress = target.getAddress() + "/upload?chunk_token=" + chunk.getToken() + "&node_token=" + target.getToken(TransferType.UPLOAD);
		Validation.validateUrl(targetAddress);
		final HttpURLConnection connection = (HttpURLConnection) new URL(this.getAddress() + "/replicate?chunk_token=" + chunk.getToken() + "&node_token=" + this.getFullToken() + "&address=" + URLEncoder.encode(targetAddress, StandardCharsets.UTF_8)).openConnection();
		connection.setRequestMethod("POST");
		if (connection.getResponseCode() == 200) {
			return true;
		} else {
			final byte[] bResponse = connection.getErrorStream().readAllBytes();
			final String response = new String(bResponse, StandardCharsets.UTF_8);
			throw new IOException("Received response code " + connection.getResponseCode() + " when trying to replicate chunk\n" + response);
		}
	}
	
	public static void processNodeAnnounce(final String token, final String version,
			final long freeSpace, final URL address, final long storageQuota,
			final String label) throws SQLException, NodeNotFoundException {
		Validate.notNull(token);
		Validate.notNull(version);
		Validate.inclusiveBetween(0, Long.MAX_VALUE, freeSpace);
		Validate.notNull(address);
		Validate.inclusiveBetween(0, Long.MAX_VALUE, storageQuota);
//		Validate.notNull(this.name);
		synchronized(NODE_LOCK) {
			if (BY_TOKEN.containsKey(token)) {
				// Node exists
				final OnlineNode node = BY_TOKEN.get(token);
				node.lastAnnounce = System.currentTimeMillis();
				node.freeSpace = freeSpace;
				node.label = label;
//				node.updateName(name);
				node.updateAddress(address);
				
//				if (node.freeSpace < Tunables.MINIMUM_FREE_SPACE_FOR_UPLOAD) {
//					if (!FREE_SPACE_NODES.contains(node)) {
//						FREE_SPACE_NODES.add(node);
//					}
//				} else {
//					FREE_SPACE_NODES.remove(node);
//				}
			} else {
				// New node
				final Optional<Node> optNode = Node.byToken(token);
				
				if (optNode.isEmpty()) {
					throw new NodeNotFoundException("Could not find node by token '" + token + "'");
				}
				
				final OnlineNode node = new OnlineNode(optNode.get());
				node.lastAnnounce = System.currentTimeMillis();
				node.freeSpace = freeSpace;
				node.label = label;
				node.updateAddress(address);
				ONLINE_NODES.add(node);
				BY_ID.put(node.getId(), node);
				BY_TOKEN.put(token, node);
//				if (onlineNode.freeSpace < Tunables.MINIMUM_FREE_SPACE_FOR_UPLOAD) {
//					if (!FREE_SPACE_NODES.contains(onlineNode)) {
//						FREE_SPACE_NODES.add(onlineNode);
//					}
//				} else {
//					FREE_SPACE_NODES.remove(onlineNode);
//				}
			}
		}
	}
	
	static void removeNode(final Node node) {
		ONLINE_NODES.remove(node);
		BY_ID.remove(node.getId());
		BY_TOKEN.remove(node.getFullToken());
	}
	
	public static void pruneNodes() {
		final Deque<OnlineNode> toRemove = new ArrayDeque<>();
		for (final OnlineNode node : ONLINE_NODES) {
			if (node.lastAnnounce + Tunables.NODE_OFFLINE_TIMEOUT < System.currentTimeMillis()) {
				toRemove.add(node);
			}
		}
		
		synchronized(NODE_LOCK) {
			while (!toRemove.isEmpty()) {
				final Node node = toRemove.pop();
				removeNode(node);
			}
		}
	}
	
	public static List<OnlineNode> getOnlineNodes() {
		synchronized(NODE_LOCK) {
			return Collections.unmodifiableList(ONLINE_NODES);
		}
	}
	
	
	public static Optional<OnlineNode> getOnlineNodeById(final long id) {
		return Optional.ofNullable(BY_ID.get(id));
	}
	
}
