package eclipfs.metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;

import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.MetaServer;
import eclipfs.metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class GetEncryptionKey extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
			if (ClientAuthentication.verify(request, response).isEmpty()) {
				return;
			}

			try (JsonWriter writer = new JsonWriter(response.getWriter())) {
				writer.beginObject();
				writer.name("key");
				writer.value(MetaServer.getEncryptionKey());
				writer.endObject();
			}
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}

}
