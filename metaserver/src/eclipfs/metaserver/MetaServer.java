package eclipfs.metaserver;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.Validate;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import eclipfs.metaserver.command.ChangeDirectoryCommand;
import eclipfs.metaserver.command.Command;
import eclipfs.metaserver.command.DeleteCommand;
import eclipfs.metaserver.command.DirectoryCreateCommand;
import eclipfs.metaserver.command.ListCommand;
import eclipfs.metaserver.command.NodeCreateCommand;
import eclipfs.metaserver.command.NodeListCommand;
import eclipfs.metaserver.command.NodeRemoveCommand;
import eclipfs.metaserver.command.ReplicateCommand;
import eclipfs.metaserver.command.ToggleWriteAccessCommand;
import eclipfs.metaserver.command.UpCommand;
import eclipfs.metaserver.command.UserAddCommand;
import eclipfs.metaserver.command.UserListCommand;
import eclipfs.metaserver.model.Directory;
import eclipfs.metaserver.model.Inode;
import eclipfs.metaserver.servlet.client.ChunkInfo;
import eclipfs.metaserver.servlet.client.ChunkTransfer;
import eclipfs.metaserver.servlet.client.DirectoryCreate;
import eclipfs.metaserver.servlet.client.FileCreate;
import eclipfs.metaserver.servlet.client.InodeDelete;
import eclipfs.metaserver.servlet.client.InodeInfo;
import eclipfs.metaserver.servlet.client.InodeMove;
import eclipfs.metaserver.servlet.node.Announce;
import eclipfs.metaserver.servlet.node.CheckGarbage;
import eclipfs.metaserver.servlet.node.NotifyChunkUploaded;

public class MetaServer {
	
	public static Directory WORKING_DIRECTORY;
	
	public static final Logger LOGGER = Logger.getGlobal();
	
	public static final PasswordEncoder PASSWORD_ENCODER = new BCryptPasswordEncoder();
	
	private static final Map<String, Command> COMMANDS = new HashMap<>();
	
	private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();
	
	public static final boolean DEBUG = true;
	
	static {
		COMMANDS.put("cd", new ChangeDirectoryCommand());
		COMMANDS.put("del", new DeleteCommand());
		COMMANDS.put("mkdir", new DirectoryCreateCommand());
		COMMANDS.put("ls", new ListCommand());
		COMMANDS.put("nodelist", new NodeListCommand());
		COMMANDS.put("nodecreate", new NodeCreateCommand());
		COMMANDS.put("noderemove", new NodeRemoveCommand());
		COMMANDS.put("replicate", new ReplicateCommand());
		COMMANDS.put("togglewriteaccess", new ToggleWriteAccessCommand());
		COMMANDS.put("up", new UpCommand());
		COMMANDS.put("useradd", new UserAddCommand());
		COMMANDS.put("userlist", new UserListCommand());
	}
	
	public static void main(final String[] args) throws Exception {
		if (!DEBUG) {
			LOGGER.setLevel(Level.WARNING);
		}

		WORKING_DIRECTORY = Inode.getRootInode();

		startWebServer();
		
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		executor.scheduleAtFixedRate(Replication::timer, 1, 30, TimeUnit.SECONDS);

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
	
	public static void runCommand(final String line) {
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
	
	private static void startWebServer() throws Exception {
		final int port = 7779;
		final String host = "0.0.0.0";
		final Server server = new Server();
		final ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
		handler.addServlet(ChunkInfo.class, "/client/chunkInfo");
		handler.addServlet(ChunkTransfer.class, "/client/chunkTransfer");
		handler.addServlet(DirectoryCreate.class, "/client/directoryCreate");
		handler.addServlet(FileCreate.class, "/client/fileCreate");
		handler.addServlet(InodeDelete.class, "/client/inodeDelete");
		handler.addServlet(InodeInfo.class, "/client/inodeInfo");
		handler.addServlet(InodeMove.class, "/client/inodeMove");
		
		handler.addServlet(Announce.class, "/node/announce");
		handler.addServlet(CheckGarbage.class, "/node/checkGarbage");
		handler.addServlet(NotifyChunkUploaded.class, "/node/notifyChunkUploaded");
		server.setHandler(handler);
        final ServerConnector connector = new ServerConnector(server);
        connector.setHost(host);
        connector.setPort(port);
        server.addConnector(connector);
        final RequestLog.Writer writer = new Slf4jRequestLogWriter();
        final RequestLog requestLog = new CustomRequestLog(writer, CustomRequestLog.NCSA_FORMAT);
        server.setRequestLog(requestLog);
		server.start();
	}

}
