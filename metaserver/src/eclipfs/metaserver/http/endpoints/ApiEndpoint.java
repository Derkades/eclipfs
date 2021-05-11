package eclipfs.metaserver.http.endpoints;

import java.io.IOException;
import java.sql.SQLException;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public abstract class ApiEndpoint {

	private final String name;
	private final RequestMethod method;

	public ApiEndpoint(final String name, final RequestMethod method) {
		this.name = name;
		this.method = method;
	}

	public String getName() {
		return this.name;
	}

	public RequestMethod getMethod() {
		return this.method;
	}

	public abstract void handle(final HttpServletRequest request, final HttpServletResponse response) throws IOException, SQLException;

	public enum RequestMethod {

		GET, POST;

	}


}
