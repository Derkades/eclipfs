package eclipfs.metaserver.http.endpoints;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.ContextHandler;

import eclipfs.metaserver.http.HttpUtil;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class EndpointHandler<Endpoint extends ApiEndpoint> extends ContextHandler {

	private final Map<String, Endpoint> endpoints = new HashMap<>();

	private final String base;

	public EndpointHandler(final String base) {
		super(null, null, base);
		Validate.isTrue(base.startsWith("/"), "base must start with /");
		this.base = base;
	}

	public void registerEndpoint(final Endpoint endpoint) {
		this.endpoints.put(endpoint.getName(), endpoint);
	}

	@Override
	public void doHandle(final String target, final Request baseRequest, final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, ServletException {
		final String uri = baseRequest.getRequestURI();
		if (!uri.startsWith(this.base)) {
			return;
		}

		baseRequest.setHandled(true);

		final String[] split = uri.split("/");
		if (split.length != 3) {
			response.sendError(403, "Invalid request path, " + split.length + " components (handler for " + this.base + ")");
			return;
		}

		final Endpoint endpoint = this.endpoints.get(split[2]);

		if (endpoint == null) {
			response.sendError(404, "Endpoint not found: " + split[2] + " (handler for " + this.base + ")");
			return;
		}

		if (!baseRequest.getMethod().equals(endpoint.getMethod().name())) {
			response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED, baseRequest.getMethod() + " method to " + endpoint.getMethod().name() + " endpoint");
			return;
		}

		try {
			endpoint.handle(request, response);
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}

}
