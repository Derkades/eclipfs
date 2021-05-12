package eclipfs.metaserver.http.endpoints.client;

import java.io.IOException;
import java.sql.SQLException;

import com.google.gson.JsonObject;

import eclipfs.metaserver.exception.AlreadyExistsException;
import eclipfs.metaserver.http.ApiError;
import eclipfs.metaserver.http.HttpUtil;
import eclipfs.metaserver.http.endpoints.ClientApiEndpoint;
import eclipfs.metaserver.model.Directory;
import eclipfs.metaserver.model.Inode;
import eclipfs.metaserver.model.User;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class InodeMove extends ClientApiEndpoint {

	public InodeMove() {
		super("inodeMove", RequestMethod.POST);
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
		final Directory newParent = HttpUtil.getJsonDirectory(json, response, "new_parent");
		final String newName = HttpUtil.getJsonString(json, response, "new_name");

		if (inode == null || newParent == null || newName == null) {
			return;
		}

		try {
			inode.move(newParent, newName);
		} catch (final AlreadyExistsException e) {
			ApiError.NAME_ALREADY_EXISTS.send(response);
			return;
		}
		HttpUtil.writeSuccessTrueJson(response);
	}

}
