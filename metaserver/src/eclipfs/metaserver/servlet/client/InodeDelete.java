package eclipfs.metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;

import com.google.gson.JsonObject;

import eclipfs.metaserver.http.endpoints.ClientApiEndpoint;
import eclipfs.metaserver.model.Directory;
import eclipfs.metaserver.model.Inode;
import eclipfs.metaserver.model.User;
import eclipfs.metaserver.servlet.ApiError;
import eclipfs.metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class InodeDelete extends ClientApiEndpoint {

	public InodeDelete() {
		super("inodeDelete", RequestMethod.POST);
	}

	@Override
	protected void handle(final User user, final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, SQLException {
		if (!user.hasWriteAccess()) {
			ApiError.MISSING_WRITE_ACCESS.send(response);
			return;
		}

		final JsonObject json = HttpUtil.readJsonFromRequestBody(request, response);

		if (json == null) {
			return;
		}

		final Inode inode = HttpUtil.getJsonInode(json, response);
		if (inode == null) {
			return;
		}

		if (inode instanceof Directory && !((Directory) inode).isEmpty()) {
			ApiError.DIRECTORY_NOT_EMPTY.send(response);
			return;
		}

		inode.delete();

		HttpUtil.writeSuccessTrueJson(response);
	}

}
