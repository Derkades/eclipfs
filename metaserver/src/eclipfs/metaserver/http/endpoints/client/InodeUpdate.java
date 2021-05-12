package eclipfs.metaserver.http.endpoints.client;

import java.io.IOException;
import java.sql.SQLException;

import com.google.gson.JsonObject;

import eclipfs.metaserver.http.HttpUtil;
import eclipfs.metaserver.http.endpoints.ClientApiEndpoint;
import eclipfs.metaserver.model.Inode;
import eclipfs.metaserver.model.User;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class InodeUpdate extends ClientApiEndpoint {

	public InodeUpdate() {
		super("inodeUpdate", RequestMethod.POST);
	}

	@Override
	protected void handle(final User user, final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, SQLException {
		final JsonObject json = HttpUtil.readJsonFromRequestBody(request, response);
		if (json == null) {
			return;
		}

		final Inode inode = HttpUtil.getJsonInode(json, response);
		if (inode == null) {
			return;
		}

		final Long modificationTime = HttpUtil.getJsonLong(json, response, "mtime");
		if (modificationTime != null) {
			inode.setModificationTime(modificationTime);
		}
		HttpUtil.writeSuccessTrueJson(response);
	}

}
