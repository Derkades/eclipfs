package eclipfs.metaserver.model;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
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

import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.MetaServer;
import eclipfs.metaserver.TransferType;
import eclipfs.metaserver.Tunables;
import eclipfs.metaserver.Validation;

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
		Validate.notNull(chunk, "Chunk is null");
		Validate.notNull(target, "Target node is null");
		final String targetAddress = target.getAddress() + "/upload" +
				"?file=" + chunk.getFile().getId() +
				"&index=" + chunk.getIndex() +
				"&node_token=" + target.getToken(TransferType.UPLOAD);
		Validation.validateUrl(targetAddress);
		final String replicateSourceAddress = this.getAddress() + "/replicate" +
				"?file=" + chunk.getFile().getId() +
				"&index=" + chunk.getIndex() +
				"&node_token=" + this.getToken() +
				"&address=" + URLEncoder.encode(targetAddress, StandardCharsets.UTF_8);
		final HttpURLConnection connection = (HttpURLConnection) new URL(replicateSourceAddress).openConnection();
		connection.setRequestMethod("POST");
		if (connection.getResponseCode() == 200) {
			return true;
		} else {
//			final byte[] bResponse = connection.getErrorStream().readAllBytes();
//			final String response = new String(bResponse, StandardCharsets.UTF_8);
//			throw new IOException("Received response code " + connection.getResponseCode() + " when trying to replicate chunk\n" + response);
			return false;
		}
	}

	public boolean finalizeUpload(final long tempId, final long chunkId) throws IOException {
		final HttpURLConnection connection = (HttpURLConnection) new URL(this.getAddress() + "/finalize").openConnection();
		connection.setRequestMethod("POST");
//		final JsonObject body = new JsonObject();
//		body.addProperty();
//		body.addProperty();
		connection.addRequestProperty("Content-Type", "application/json");
		connection.setDoOutput(true);
		try (JsonWriter writer = new JsonWriter(new OutputStreamWriter(connection.getOutputStream()))) {
			writer.beginObject();
			writer.name("temp_id").value(tempId);
			writer.name("chunk_id").value(chunkId);
			writer.endObject();
		}
//		connection.getOutputStream().write(StandardCharsets.UTF_8.encode(body.toString()).array());
		if (connection.getResponseCode() == 200) {
			return true;
		} else {
			MetaServer.LOGGER.error("Response code " + connection.getResponseCode() + " while trying to finalize a chunk upload");
			MetaServer.LOGGER.error(readStream(connection.getErrorStream()));
			return false;
		}
	}

	private static String readStream(final InputStream stream) throws IOException {
		final byte[] bResponse = stream.readAllBytes();
		final String response = new String(bResponse, StandardCharsets.UTF_8);
		return response;
	}

	public static void processNodeAnnounce(final Node node, final URL address,
			final String version, final long freeSpace, final long storageQuota) throws SQLException {
		Validate.notNull(node, "Node is null");
		Validate.notNull(version, "Version is null");
		Validate.notNull(address, "Address is null");
		Validate.inclusiveBetween(0, Long.MAX_VALUE, freeSpace, "Free space must be >= 0");
		Validate.inclusiveBetween(0, Long.MAX_VALUE, storageQuota, "Storage quota must be >= 0");
		synchronized(NODE_LOCK) {
			if (BY_TOKEN.containsKey(node.getToken())) {
				// Node exists
				final OnlineNode online = BY_TOKEN.get(node.getToken());
				online.lastAnnounce = System.currentTimeMillis();
				online.address = address;
				online.version = version;
				online.freeSpace = freeSpace;
				online.storageQuota = storageQuota;
			} else {
				// New node
				final OnlineNode online = new OnlineNode(node);
				online.lastAnnounce = System.currentTimeMillis();
				online.address = address;
				online.version = version;
				online.freeSpace = freeSpace;
				online.storageQuota = storageQuota;

				ONLINE_NODES.add(online);
				BY_ID.put(node.getId(), online);
				BY_TOKEN.put(node.getToken(), online);
			}
		}
	}

	static void removeNode(final Node node) {
		Validate.notNull(node);
		ONLINE_NODES.remove(node);
		BY_ID.remove(node.getId());
		BY_TOKEN.remove(node.getToken());
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
