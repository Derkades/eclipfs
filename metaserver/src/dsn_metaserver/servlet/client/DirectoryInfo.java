package dsn_metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import com.google.gson.stream.JsonWriter;

import dsn_metaserver.model.Directory;
import dsn_metaserver.model.File;
import dsn_metaserver.servlet.ApiError;
import dsn_metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class DirectoryInfo extends HttpServlet {
	
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
//			final Optional<User> optUser = ClientAuthentication.verify(request, response);
//			if (optUser.isEmpty()) {
//				return;
//			}
			
			final String path = HttpUtil.getStringParameter(request, response, "path");
			if (path == null) {
				return;
			}
			
			final Optional<Directory> optDir = Directory.findByPath(path);
		
			if (optDir.isEmpty()) {
				ApiError.DIRECTORY_NOT_EXISTS.send(response);
				return;
			}
			
			final Directory directory = optDir.get();
			
			try (JsonWriter jsonResponse = new JsonWriter(response.getWriter())) {
				jsonResponse.beginObject();
				jsonResponse.name("directory");
				DirectoryInfo.writeDirectoryInfoJson(directory, jsonResponse);
				jsonResponse.endObject();
			}
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
		}
	}
	
	static void writeDirectoryInfoJson(final Directory directory, final JsonWriter json) throws IOException, SQLException {
		json.beginObject();
		
		json.name("name").value(directory.getName());
		
		final Optional<Directory> optParent = directory.getParent();
		if (optParent.isPresent()) {
			final Directory parent = optParent.get();
			json.name("parent_id").value(parent.getId());
			json.name("parent_name").value(parent.getName());
		}
		
		json.name("directories");
		json.beginArray();
		for (final Directory subDirectory : directory.listDirectories()) {
			json.value(subDirectory.getName());
		}
		json.endArray();

		json.name("files");
		json.beginArray();
		for (final File file : directory.listFiles()) {
			FileInfo.writeFileInfoJson(file, json);
		}
		json.endArray();
		
		json.endObject();
	}

}
