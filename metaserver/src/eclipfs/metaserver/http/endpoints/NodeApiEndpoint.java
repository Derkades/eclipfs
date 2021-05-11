package eclipfs.metaserver.http.endpoints;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import eclipfs.metaserver.Tunables;
import eclipfs.metaserver.model.Node;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public abstract class NodeApiEndpoint extends ApiEndpoint {

	public NodeApiEndpoint(final String name, final RequestMethod method) {
		super(name, method);
	}

	@Override
	public void handle(final HttpServletRequest request, final HttpServletResponse response) throws IOException, SQLException {
		final String token = request.getHeader("X-DSN-NodeToken");

		if (token == null) {
			response.setStatus(HttpServletResponse.SC_FORBIDDEN);
			response.setContentType("text/plain");
			response.getWriter().write("Node token not specified");
			return;
		}

		if (token.length() != Tunables.NODE_TOKEN_LENGTH) {
			response.setStatus(HttpServletResponse.SC_FORBIDDEN);
			response.setContentType("text/plain");
			response.getWriter().write("Node token has invalid length");
		}

		final Optional<Node> optNode = Node.byToken(token);
		if (optNode.isPresent()) {
			handle(optNode.get(), request, response);
		}

		response.setStatus(HttpServletResponse.SC_FORBIDDEN);
		response.setContentType("text/plain");
		response.getWriter().write("Invalid node token '" + token + "'");
	}

	protected abstract void handle(Node node, final HttpServletRequest request, final HttpServletResponse response) throws IOException, SQLException;

}
