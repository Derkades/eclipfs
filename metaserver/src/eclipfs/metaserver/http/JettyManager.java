package eclipfs.metaserver.http;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.sql.SQLException;

import org.eclipse.jetty.io.ConnectionStatistics;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;

import eclipfs.metaserver.MetaServer;
import eclipfs.metaserver.servlet.client.ChunkInfo;
import eclipfs.metaserver.servlet.client.ChunkTransfer;
import eclipfs.metaserver.servlet.client.DirectoryCreate;
import eclipfs.metaserver.servlet.client.FileCreate;
import eclipfs.metaserver.servlet.client.GetEncryptionKey;
import eclipfs.metaserver.servlet.client.InodeDelete;
import eclipfs.metaserver.servlet.client.InodeInfo;
import eclipfs.metaserver.servlet.client.InodeMove;
import eclipfs.metaserver.servlet.dashboard.Dashboard;
import eclipfs.metaserver.servlet.node.Announce;
import eclipfs.metaserver.servlet.node.CheckGarbage;
import eclipfs.metaserver.servlet.node.NotifyChunkUploaded;

public class JettyManager {

	private final HttpSecurityManager security = new HttpSecurityManager();
	private final Server server  = new Server();
	final ConnectionStatistics stats = new ConnectionStatistics();

	public JettyManager(final int port) throws MalformedURLException, SQLException, URISyntaxException {
		final ServletContextHandler clientApiContext = new ServletContextHandler();
		clientApiContext.setContextPath("/client");
		clientApiContext.addServlet(ChunkInfo.class, "/chunkInfo");
		clientApiContext.addServlet(ChunkTransfer.class, "/chunkTransfer");
		clientApiContext.addServlet(DirectoryCreate.class, "/directoryCreate");
		clientApiContext.addServlet(FileCreate.class, "/fileCreate");
		clientApiContext.addServlet(GetEncryptionKey.class, "/getEncryptionKey");
		clientApiContext.addServlet(InodeDelete.class, "/inodeDelete");
		clientApiContext.addServlet(InodeInfo.class, "/inodeInfo");
		clientApiContext.addServlet(InodeMove.class, "/inodeMove");
		clientApiContext.setServer(this.server);

		final ServletContextHandler nodeApiContext = new ServletContextHandler();
		nodeApiContext.setContextPath("/node");
		nodeApiContext.addServlet(Announce.class, "/announce");
		nodeApiContext.addServlet(CheckGarbage.class, "/checkGarbage");
		nodeApiContext.addServlet(NotifyChunkUploaded.class, "/notifyChunkUploaded");
		nodeApiContext.setServer(this.server);

		final ServletContextHandler dashboardContext = new ServletContextHandler();
		dashboardContext.setContextPath("/dashboard");
		dashboardContext.addServlet(Dashboard.class, "/");
		dashboardContext.setServer(this.server);
		dashboardContext.setSecurityHandler(this.security.getSecurityHandler());

		final ServletContextHandler rootContext = new ServletContextHandler();
		rootContext.setContextPath("/");
		final ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);
		rootContext.addServlet(holderPwd, "/");
		rootContext.setWelcomeFiles(new String[] { "index.html" });
		rootContext.setBaseResource(Resource.newResource(MetaServer.class.getResource("/web/styles.css").toURI().resolve("./").normalize()));
		rootContext.setServer(this.server);

		final ContextHandlerCollection list = new ContextHandlerCollection();
		list.addHandler(clientApiContext);
		list.addHandler(nodeApiContext);
		list.addHandler(dashboardContext);
		list.addHandler(rootContext);
		list.setServer(this.server);
		this.server.setHandler(list);

		final ServerConnector connector = new ServerConnector(this.server);
		connector.setPort(port);
		this.server.addConnector(connector);
		final RequestLog.Writer writer = new Slf4jRequestLogWriter();
		final RequestLog requestLog = new CustomRequestLog(writer, CustomRequestLog.NCSA_FORMAT);
		this.server.setRequestLog(requestLog);

		connector.addBean(this.stats);
	}

	public void start() throws Exception {
		this.server.start();
	}

	public ConnectionStatistics getStatistics() {
		return this.stats;
	}

	public HttpSecurityManager getSecurityManager() {
		return this.security;
	}

}
