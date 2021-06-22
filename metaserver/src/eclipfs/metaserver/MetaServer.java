package eclipfs.metaserver;

import java.net.http.HttpClient;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.apache.commons.lang3.Validate;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eclipfs.metaserver.command.ChangeDirectoryCommand;
import eclipfs.metaserver.command.Command;
import eclipfs.metaserver.command.DeleteCommand;
import eclipfs.metaserver.command.DirectoryCreateCommand;
import eclipfs.metaserver.command.ListCommand;
import eclipfs.metaserver.command.NodeCreateCommand;
import eclipfs.metaserver.command.NodeListCommand;
import eclipfs.metaserver.command.NodeRemoveCommand;
import eclipfs.metaserver.command.ToggleWriteAccessCommand;
import eclipfs.metaserver.command.UpCommand;
import eclipfs.metaserver.command.UserAddCommand;
import eclipfs.metaserver.command.UserListCommand;
import eclipfs.metaserver.http.JettyManager;
import eclipfs.metaserver.http.PasswordChecker;
import eclipfs.metaserver.migration.Migrations;
import eclipfs.metaserver.model.Directory;
import eclipfs.metaserver.model.Inode;

public class MetaServer {

	public static Directory WORKING_DIRECTORY;

	public static final Logger LOGGER = LoggerFactory.getLogger("Metaserver");
	private static final Map<String, Command> COMMANDS = new HashMap<>();
	static {
		COMMANDS.put("cd", new ChangeDirectoryCommand());
		COMMANDS.put("del", new DeleteCommand());
		COMMANDS.put("mkdir", new DirectoryCreateCommand());
		COMMANDS.put("ls", new ListCommand());
		COMMANDS.put("nodelist", new NodeListCommand());
		COMMANDS.put("nodecreate", new NodeCreateCommand());
		COMMANDS.put("noderemove", new NodeRemoveCommand());
		COMMANDS.put("togglewriteaccess", new ToggleWriteAccessCommand());
		COMMANDS.put("up", new UpCommand());
		COMMANDS.put("useradd", new UserAddCommand());
		COMMANDS.put("userlist", new UserListCommand());
	}

	private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();

	public static final boolean DEBUG = true;

	private static final String ENCRYPTION_KEY_ENCODED;

	private static JettyManager httpServer;

	private static PasswordChecker passwordChecker = new PasswordChecker();

	// These must never be changed or existing encrypted data will become inaccessible
	private static final byte[] PBKDF2_SALT = "1N8Dx]#%O6)d.ezyGTeIHi)Z=v+rH7|{c.^yOy52>[(<[Lnmb~<}\\d`0.**)tt%H".getBytes();
	private static final int PBKDF2_ITER = 100000;

	private static final int defaultChunkSize;

	static {
		String keyString = System.getenv("ENCRYPTION_KEY");
		if (keyString == null || keyString.equals("")) {
			LOGGER.warn("ENCRYPTION_KEY is not set or blank. If this is a production installation, set this variable immediately before uploading any files.");
			keyString = "this is an insecure password";
		}

		// Convert human friendly password to a 32-byte key for AES

		final KeySpec spec = new PBEKeySpec(keyString.toCharArray(), PBKDF2_SALT, PBKDF2_ITER, 256);
		final byte[] key;
		try {
			final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
			key = factory.generateSecret(spec).getEncoded();
		} catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
			throw new RuntimeException(e);
		}
		Validate.isTrue(key.length == 32);
		ENCRYPTION_KEY_ENCODED = Base64.getEncoder().encodeToString(key);

		defaultChunkSize = System.getenv("CHUNK_SIZE") != null ? Integer.parseInt(System.getenv("CHUNK_SIZE")) : 1_000_000;
		LOGGER.info("Using chunk size " + defaultChunkSize);
	}

	public static void main(final String[] args) throws Exception {
		Migrations.runMigrations();

		WORKING_DIRECTORY = Inode.getRootInode();

		httpServer = new JettyManager(7779); // TODO configurable port
		httpServer.start();

		THREAD_POOL.execute(Replication::run);

		final LineReader reader = LineReaderBuilder.builder().build();
		while (true) {
			try {
				final String line = reader.readLine(getPrompt());
				runCommand(line);
			} catch (final UserInterruptException e) {
			} catch (final EndOfFileException e) {
				return;
			}
		}
	}

	private static String getPrompt() throws SQLException {
		return "dsn " + WORKING_DIRECTORY.getAbsolutePath() + " > ";
	}

	private static void runCommand(final String line) {
		Validate.notNull(line);
		final String[] split = line.split(" ");
		final String name = split[0];
		final Command command = COMMANDS.get(name);
		if (command == null) {
			System.out.println("Unknown command");
			for (final String name2 : COMMANDS.keySet()) {
				System.out.println(" - " + name2);
			}
		} else {
			final String[] args = Arrays.copyOfRange(split, 1, split.length);
			try {
				command.run(args);
			} catch (final Exception e) {
				if (DEBUG) {
					e.printStackTrace();
				} else {
					System.out.println("An error occured: " + e.getClass().getSimpleName() + " - " +  e.getMessage());
				}
			}
		}
	}

	public static JettyManager getHttpServer() {
		return httpServer;
	}

	public static String getEncryptionKeyBase64() {
		return ENCRYPTION_KEY_ENCODED;
	}

	public static PasswordChecker getPasswordChecker() {
		return passwordChecker;
	}

	private static final HttpClient httpClient = HttpClient.newBuilder()
			.connectTimeout(Duration.ofSeconds(10))
			.executor(THREAD_POOL)
			.build();

	public static HttpClient getHttpClient() {
		return httpClient;
	}

	public static int getDefaultChunkSize() {
		return defaultChunkSize;
	}

}
