package dsn_metaserver;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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

import dsn_metaserver.command.ChangeDirectoryCommand;
import dsn_metaserver.command.Command;
import dsn_metaserver.command.DeleteCommand;
import dsn_metaserver.command.DirectoryCreateCommand;
import dsn_metaserver.command.ListCommand;
import dsn_metaserver.command.NodeCreateCommand;
import dsn_metaserver.command.NodeListCommand;
import dsn_metaserver.command.NodeRemoveCommand;
import dsn_metaserver.command.ReplicateCommand;
import dsn_metaserver.command.ToggleWriteAccessCommand;
import dsn_metaserver.command.UpCommand;
import dsn_metaserver.command.UserAddCommand;
import dsn_metaserver.command.UserListCommand;
import dsn_metaserver.model.Directory;
import dsn_metaserver.model.Inode;
import dsn_metaserver.servlet.client.ChunkInfo;
import dsn_metaserver.servlet.client.ChunkTransfer;
import dsn_metaserver.servlet.client.DirectoryCreate;
import dsn_metaserver.servlet.client.DirectoryDelete;
import dsn_metaserver.servlet.client.FileCreate;
import dsn_metaserver.servlet.client.InodeInfo;
import dsn_metaserver.servlet.client.Move;
import dsn_metaserver.servlet.node.Announce;
import dsn_metaserver.servlet.node.NotifyChunkUploaded;

public class MetaServer {
	
	public static Directory WORKING_DIRECTORY;
	
	public static final Logger LOGGER = Logger.getGlobal();
	
	public static final PasswordEncoder PASSWORD_ENCODER = new BCryptPasswordEncoder();
	
	private static final Map<String, Command> COMMANDS = new HashMap<>();
	
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
		handler.addServlet(DirectoryDelete.class, "/client/directoryDelete");
		handler.addServlet(FileCreate.class, "/client/fileCreate");
		handler.addServlet(InodeInfo.class, "/client/nodeInfo");
		handler.addServlet(Move.class, "/client/directoryMove");
		
		handler.addServlet(Announce.class, "/node/announce");
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
