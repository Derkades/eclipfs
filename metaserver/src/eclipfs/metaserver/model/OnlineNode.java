package eclipfs.metaserver.model;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
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
import org.slf4j.Logger;

import com.google.gson.JsonObject;

import eclipfs.metaserver.MetaServer;
import eclipfs.metaserver.TransferType;
import eclipfs.metaserver.Tunables;
import xyz.derkades.derkutils.UriBuilder;

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

	public boolean requestReplicate(final Chunk chunk, final OnlineNode source, final Logger logger) {
		Validate.notNull(chunk, "Chunk is null");
		Validate.notNull(source, "Source node is null");

		try {
			final String sourceAddress = new UriBuilder(source.getAddress())
					.slash("download")
					.param("chunk", String.valueOf(chunk.getId()))
					.param("node_token", source.getToken(TransferType.DOWNLOAD))
					.toString();

			final URI uri = new UriBuilder(this.getAddress())
					.slash("replicate")
					.param("chunk", String.valueOf(chunk.getId()))
					.param("checksum", chunk.getChecksumHex())
					.param("node_token", this.getToken())
					.param("address", sourceAddress)
					.build();


			final HttpRequest request = HttpRequest.newBuilder(uri).POST(BodyPublishers.noBody()).build();
			final HttpResponse<String> response = MetaServer.getHttpClient().send(request, BodyHandlers.ofString());
			if (response.statusCode() == 200) {
				return true;
			} else {
				logger.warn("Received response code %s", response.statusCode());
				logger.warn("Address: %s", this.address);
				if (response.body().length() < 1000) {
					logger.warn("Response: %s", response.body());
				} else {
					logger.warn("Response is too long to print");
				}
				return false;
			}
		} catch (final IOException | InterruptedException e) {
			logger.warn("Error", e);
			logger.warn("Address: %s", this.address);
			return false;
		}
	}

	public boolean finalizeUpload(final long tempId, final long chunkId, final Logger logger) throws IOException {
		final URI uri = new UriBuilder(this.getAddress()).slash("finalize").build();

		final JsonObject json = new JsonObject();
		json.addProperty("temp_id", tempId);
		json.addProperty("chunk_id", chunkId);

		final HttpRequest request = HttpRequest.newBuilder(uri)
				.header("Content-Type", "application/json")
				.POST(BodyPublishers.ofString(json.toString()))
				.build();

		try {
			final HttpResponse<String> response = MetaServer.getHttpClient().send(request, BodyHandlers.ofString());
			if (response.statusCode() == 200) {
				return true;
			} else {
				logger.error("Response code " + response.statusCode() + " while trying to finalize a chunk upload");
				if (response.body().length() < 1000) {
					logger.warn("Response: %s", response.body());
				} else {
					logger.warn("Response is too long to print");
				}
				return false;
			}
		} catch (IOException | InterruptedException e) {
			logger.error("Error during request", e);
			return false;
		}
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
