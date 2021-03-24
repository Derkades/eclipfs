package eclipfs.metaserver;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.Validate;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;

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
import eclipfs.metaserver.model.Directory;
import eclipfs.metaserver.model.Inode;

public class MetaServer {

	public static Directory WORKING_DIRECTORY;

	public static final Logger LOGGER = Logger.getGlobal();
	private static final Map<String, Command> COMMANDS = new HashMap<>();

	private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();

	public static final boolean DEBUG = true;

	private static final String ENCRYPTION_KEY;

	private static JettyManager httpServer;

	private static PasswordChecker passwordChecker = new PasswordChecker();

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

		final String key = System.getenv("ENCRYPTION_KEY");
		Validate.notEmpty(key, "Environment variable ENCRYPTION_KEY is not set or empty");
		Validate.isTrue(key.length() >= 32, "Encryption password must be at least 32 characters");
		if (key.length() > 32) {
			System.err.println("Encryption key is longer than 32 characters. Please note that any characters after 32 are ignored.");
		}
		ENCRYPTION_KEY = key.substring(0, 32);
	}

	public static void main(final String[] args) throws Exception {
		if (!DEBUG) {
			LOGGER.setLevel(Level.WARNING);
		}

		WORKING_DIRECTORY = Inode.getRootInode();

		httpServer = new JettyManager(7779); // TODO configurable port
		httpServer.start();

//		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
//		executor.scheduleAtFixedRate(Replication::timer, 30, 30, TimeUnit.SECONDS);

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

	public static String getEncryptionKey() {
		return ENCRYPTION_KEY;
	}

	public static PasswordChecker getPasswordChecker() {
		return passwordChecker;
	}

}
