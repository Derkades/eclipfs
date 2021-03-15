package dsn_metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;

import com.google.gson.stream.JsonWriter;

import dsn_metaserver.model.Directory;
import dsn_metaserver.model.File;
import dsn_metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class FileInfo extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
			if (ClientAuthentication.verify(request, response).isEmpty()) {
				return;
			}
			
			final File file = HttpUtil.getFileInodeParameter(request, response);
			if (file == null) {
				return;
			}
			
			try (JsonWriter jsonResponse = HttpUtil.getJsonWriter(response)) {
				jsonResponse.beginObject();
				jsonResponse.name("file");
				FileInfo.writeFileInfoJson(file, jsonResponse);
				jsonResponse.endObject();
			}
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}
	
	static void writeFileInfoJson(final File file, final JsonWriter json) throws IOException, SQLException {
		final Directory dir = file.getParent();
		json.beginObject()
				.name("name").value(file.getName())
				.name("dir").value(dir.getAbsolutePath())
				.name("size").value(file.getSize())
				.endObject();
	}

}
