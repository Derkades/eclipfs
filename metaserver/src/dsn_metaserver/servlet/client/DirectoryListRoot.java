package dsn_metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import com.google.gson.stream.JsonWriter;

import dsn_metaserver.model.Directory;
import dsn_metaserver.model.User;
import dsn_metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class DirectoryListRoot extends HttpServlet {
	
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
			final Optional<User> optUser = ClientAuthentication.verify(request, response);
				
			if (optUser.isEmpty()) {
				return;
			}
			
			final List<Directory> directories = Directory.getRootDirectories();
			
			try (JsonWriter json = new JsonWriter(response.getWriter())) {
				json.beginObject();
				json.name("directories");
				json.beginArray();
				for (final Directory directory : directories) {
					DirectoryInfo.writeDirectoryInfoJson(directory, json);
				}
				json.endArray();
				json.endObject();
			}
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}

}
