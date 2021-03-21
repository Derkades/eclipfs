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

	private long lastAnnounce = -1;
	private String version = null;
	private URL address = null;
	private long storageQuota = -1;
	private long freeSpace = -1;

	public OnlineNode(final Node node) {
		super(node.id, node.token, node.location, node.name);
	}

	public long getLastAnnounce() {
		Validate.isTrue(this.lastAnnounce >= 0);
		return this.lastAnnounce;
	}

	private boolean isOnline() {
		return this.getLastAnnounce() + Tunables.NODE_OFFLINE_TIMEOUT > System.currentTimeMillis();
	}

	public URL getAddress() {
		Validate.notNull(this.address);
		return this.address;
	}

	public String getVersion() {
		Validate.notNull(this.version);
		return this.version;
	}

	public long getFreeSpace() {
		Validate.isTrue(this.freeSpace >= 0);
		return this.freeSpace;
	}

	public long getStorageQuota() {
		Validate.isTrue(this.storageQuota >= 0);
		return this.storageQuota;
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

	public static void processNodeAnnounce(final String token, final URL address,
			final String version, final long freeSpace, final long storageQuota) throws SQLException, NodeNotFoundException {
		Validate.notNull(token);
		Validate.notNull(version);
		Validate.notNull(address);
		Validate.inclusiveBetween(0, Long.MAX_VALUE, freeSpace);
		Validate.inclusiveBetween(0, Long.MAX_VALUE, storageQuota);
		synchronized(NODE_LOCK) {
			if (BY_TOKEN.containsKey(token)) {
				// Node exists
				final OnlineNode node = BY_TOKEN.get(token);
				node.lastAnnounce = System.currentTimeMillis();
				node.address = address;
				node.version = version;
				node.freeSpace = freeSpace;
				node.storageQuota = storageQuota;
			} else {
				// New node
				final Optional<Node> optNode = Node.byToken(token);

				if (optNode.isEmpty()) {
					throw new NodeNotFoundException("Could not find node by token '" + token + "'");
				}

				final OnlineNode node = new OnlineNode(optNode.get());
				node.lastAnnounce = System.currentTimeMillis();
				node.address = address;
				node.version = version;
				node.freeSpace = freeSpace;
				node.storageQuota = storageQuota;

				ONLINE_NODES.add(node);
				BY_ID.put(node.getId(), node);
				BY_TOKEN.put(token, node);
			}
		}
	}

	static void removeNode(final Node node) {
		Validate.notNull(node);
		ONLINE_NODES.remove(node);
		BY_ID.remove(node.getId());
		BY_TOKEN.remove(node.getFullToken());
	}

	private static void pruneNodes() {
		synchronized(NODE_LOCK) {
			final Deque<OnlineNode> toRemove = new ArrayDeque<>();
			for (final OnlineNode node : ONLINE_NODES) {
				if (!node.isOnline()) {
					toRemove.add(node);
				}
			}

			while (!toRemove.isEmpty()) {
				final OnlineNode node = toRemove.pop();
				removeNode(node);
			}
		}
	}

	public static List<OnlineNode> getOnlineNodes() {
		synchronized(NODE_LOCK) {
			pruneNodes();
			return Collections.unmodifiableList(ONLINE_NODES);
		}
	}

	public static Optional<OnlineNode> getOnlineNodeById(final long id) {
		synchronized(NODE_LOCK) {
			OnlineNode node = BY_ID.get(id);

			if (node != null && !node.isOnline()) {
				removeNode(node);
				node = null;
			}

			return Optional.ofNullable(node);
		}
	}

}
