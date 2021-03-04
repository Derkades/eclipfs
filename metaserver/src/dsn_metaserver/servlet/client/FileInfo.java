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

public class FileInfo extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
			if (ClientAuthentication.verify(request, response).isEmpty()) {
				return;
			}
			
			final String directoryPath = HttpUtil.getStringParameter(request, response, "directory");
			final String fileName = HttpUtil.getStringParameter(request, response, "name");
			if (directoryPath == null || fileName == null) {
				return;
			}
					
			final Optional<Directory> directory = Directory.findByPath(directoryPath);
			
			if (directory.isEmpty()) {
				ApiError.DIRECTORY_NOT_EXISTS.send(response);
				return;
			}
			
			final Optional<File> fileOpt = directory.get().getFile(fileName);
			
			if (fileOpt.isEmpty()) {
				ApiError.FILE_NOT_EXISTS.send(response);
				return;
			}
			
			final File file = fileOpt.get();
			
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
		final Directory dir = file.getDirectory();
		json.beginObject()
				.name("name").value(file.getName())
				.name("dir").value(dir.getAboslutePath())
				.name("size").value(file.getSize())
				.endObject();
	}

}
