package eclipfs.metaserver.http.endpoints;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Optional;

import eclipfs.metaserver.model.User;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public abstract class ClientApiEndpoint extends ApiEndpoint {

	public ClientApiEndpoint(final String name, final RequestMethod method) {
		super(name, method);
	}

	@Override
	public void handle(final HttpServletRequest request, final HttpServletResponse response) throws IOException, SQLException {
		final String encodedUsername = request.getHeader("X-DSN-Username");
		final String encodedPassword = request.getHeader("X-DSN-Password");

		if (encodedUsername == null || encodedPassword == null) {
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
			response.setContentType("text/plain");
			response.getWriter().write("Username or password not specified");
			return;
		}

		final Decoder decoder = Base64.getDecoder();
		final String username = new String(decoder.decode(encodedUsername), StandardCharsets.UTF_8);

		final Optional<User> optUser = User.get(username);

		if (optUser.isPresent()) {
			final String password = new String(decoder.decode(encodedPassword), StandardCharsets.UTF_8);
			if (optUser.get().verifyPassword(password)) {
				handle(optUser.get(), request, response);
				return;
			}
		}

		response.setStatus(HttpServletResponse.SC_FORBIDDEN);
		response.setContentType("text/plain");
		response.getWriter().write("Invalid username or password");
	}

	protected abstract void handle(User user, final HttpServletRequest request, final HttpServletResponse response) throws IOException, SQLException;

}
