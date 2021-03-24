package eclipfs.metaserver.servlet.node;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import eclipfs.metaserver.Tunables;
import eclipfs.metaserver.model.Node;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class NodeAuthentication {

	public static Optional<Node> verify(final HttpServletRequest request, final HttpServletResponse response) throws IOException, SQLException {
		final String token = request.getHeader("X-DSN-NodeToken");

		if (token == null) {
			response.setStatus(HttpServletResponse.SC_FORBIDDEN);
			response.setContentType("text/plain");
			response.getWriter().write("Node token not specified");
			return Optional.empty();
		}

		if (token.length() != Tunables.NODE_TOKEN_LENGTH) {
			response.setStatus(HttpServletResponse.SC_FORBIDDEN);
			response.setContentType("text/plain");
			response.getWriter().write("Node token has invalid length");
		}

		final Optional<Node> optNode = Node.byToken(token);
		if (optNode.isPresent()) {
			return optNode;
		}

		response.setStatus(HttpServletResponse.SC_FORBIDDEN);
		response.setContentType("text/plain");
		response.getWriter().write("Invalid node token '" + token + "'");
		return Optional.empty();
	}

}
