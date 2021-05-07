package eclipfs.metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;

import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.model.Directory;
import eclipfs.metaserver.model.Inode;
import eclipfs.metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class InodeInfo extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
			if (ClientAuthentication.verify(request, response).isEmpty()) {
				return;
			}

			final Inode inode = HttpUtil.getInodeParameter(request, response);

			if (inode == null) {
				return;
			}

			try (JsonWriter writer = HttpUtil.getJsonWriter(response)) {
				writer.beginObject();

				writeInodeInfoJson(inode, writer);
				if (!inode.isFile()) {
					final Directory directory = (Directory) inode;
					writer.name("children");
					directory.writeEntriesAsJsonDictionary(writer);
				}

				writer.endObject();
			}

		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
			return;
		}
	}

	static void writeInodeInfoJson(final Inode inode, final JsonWriter writer) throws IOException, SQLException {
		writer.name("inode").value(inode.getId());
		writer.name("name").value(inode.getName());
		writer.name("path").value(inode.getAbsolutePath());
		writer.name("type").value(inode.isFile() ? "f" : "d");
		writer.name("size").value(inode.getSize());
		writer.name("crtime").value(inode.getCrtime());
		writer.name("mtime").value(inode.getMtime());
		writer.name("parent").value(inode.getParentId());
	}

}
