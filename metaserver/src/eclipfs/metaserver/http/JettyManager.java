package eclipfs.metaserver.http;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
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
import eclipfs.metaserver.http.endpoints.ClientApiEndpoint;
import eclipfs.metaserver.http.endpoints.EndpointHandler;
import eclipfs.metaserver.http.endpoints.NodeApiEndpoint;
import eclipfs.metaserver.servlet.client.ChunkDownload;
import eclipfs.metaserver.servlet.client.ChunkInfo;
import eclipfs.metaserver.servlet.client.ChunkUploadFinalize;
import eclipfs.metaserver.servlet.client.ChunkUploadInit;
import eclipfs.metaserver.servlet.client.DirectoryCreate;
import eclipfs.metaserver.servlet.client.FileCreate;
import eclipfs.metaserver.servlet.client.GetEncryptionKey;
import eclipfs.metaserver.servlet.client.InodeDelete;
import eclipfs.metaserver.servlet.client.InodeInfo;
import eclipfs.metaserver.servlet.client.InodeMove;
import eclipfs.metaserver.servlet.client.StatFilesystem;
import eclipfs.metaserver.servlet.dashboard.DashboardFilesystem;
import eclipfs.metaserver.servlet.dashboard.DashboardNodes;
import eclipfs.metaserver.servlet.dashboard.DashboardReplication;
import eclipfs.metaserver.servlet.dashboard.DashboardUsers;
import eclipfs.metaserver.servlet.node.Announce;
import eclipfs.metaserver.servlet.node.CheckGarbage;

public class JettyManager {

	private final HttpSecurityManager dashboardSecurity = new HttpSecurityManager();
	private final Server server  = new Server();
	final ConnectionStatistics stats = new ConnectionStatistics();

	public JettyManager(final int port) throws MalformedURLException, SQLException, URISyntaxException {
		final EndpointHandler<ClientApiEndpoint> clientEndpoints = new EndpointHandler<>("/client");
		clientEndpoints.registerEndpoint(new ChunkDownload());
		clientEndpoints.registerEndpoint(new ChunkInfo());
		clientEndpoints.registerEndpoint(new ChunkUploadFinalize());
		clientEndpoints.registerEndpoint(new ChunkUploadInit());
		clientEndpoints.registerEndpoint(new DirectoryCreate());
		clientEndpoints.registerEndpoint(new FileCreate());
		clientEndpoints.registerEndpoint(new GetEncryptionKey());
		clientEndpoints.registerEndpoint(new InodeDelete());
		clientEndpoints.registerEndpoint(new InodeInfo());
		clientEndpoints.registerEndpoint(new InodeMove());
		clientEndpoints.registerEndpoint(new StatFilesystem());

		final EndpointHandler<NodeApiEndpoint> nodeEndpoints = new EndpointHandler<>("/node");
		nodeEndpoints.registerEndpoint(new Announce());
		nodeEndpoints.registerEndpoint(new CheckGarbage());

		final ServletContextHandler dashboardContext = new ServletContextHandler();
		dashboardContext.setContextPath("/dashboard");
		final ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);
		dashboardContext.addServlet(holderPwd, "/");
		dashboardContext.setWelcomeFiles(new String[] { "index.html" });
		final String dir = "/dashboard";
		final String someFile = "/styles.css";
		final URL randomFileUrl = MetaServer.class.getResource(dir + someFile);
		final Resource baseResource = Resource.newResource(new URL(StringUtils.removeEnd(randomFileUrl.toString(), someFile)));
		dashboardContext.setBaseResource(baseResource);
		dashboardContext.addServlet(DashboardFilesystem.class, "/filesystem");
		dashboardContext.addServlet(DashboardNodes.class, "/nodes");
		dashboardContext.addServlet(DashboardReplication.class, "/replication");
		dashboardContext.addServlet(DashboardUsers.class, "/users");
		dashboardContext.setSecurityHandler(this.dashboardSecurity.getSecurityHandler());

		final ContextHandlerCollection handlers = new ContextHandlerCollection();
		handlers.addHandler(clientEndpoints);
		handlers.addHandler(nodeEndpoints);
		handlers.addHandler(dashboardContext);
		this.server.setHandler(handlers);

		final ServerConnector connector = new ServerConnector(this.server);
		connector.setPort(port);
		this.server.addConnector(connector);
		final Slf4jRequestLogWriter writer = new Slf4jRequestLogWriter();
		writer.setLoggerName("HTTP");
		final RequestLog requestLog = new CustomRequestLog(writer, "%{ms}Tms %r %s %O %{client}a");
		this.server.setRequestLog(requestLog);
		connector.addBean(this.stats);
	}

	public void start() throws Exception {
		this.server.start();
	}

	public ConnectionStatistics getStatistics() {
		return this.stats;
	}

	public HttpSecurityManager getDashboardSecurityManager() {
		return this.dashboardSecurity;
	}

}
