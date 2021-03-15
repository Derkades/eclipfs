package dsn_metaserver.servlet.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import com.google.gson.JsonObject;

import dsn_metaserver.exception.AlreadyExistsException;
import dsn_metaserver.model.Directory;
import dsn_metaserver.model.Inode;
import dsn_metaserver.model.User;
import dsn_metaserver.servlet.ApiError;
import dsn_metaserver.servlet.HttpUtil;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class Move extends HttpServlet {
	
	private static final long serialVersionUID = 1L;

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		try {
			final Optional<User> optUser = ClientAuthentication.verify(request, response);
			
			if (optUser.isEmpty()) {
				return;
			}
			
			final User user = optUser.get();
			
			if (!user.hasWriteAccess()) {
				ApiError.MISSING_WRITE_ACCESS.send(response);
				return;
			}
			
			final JsonObject json = HttpUtil.readJsonFromRequestBody(request, response);
			
			if (json == null) {
				return;
			}
			
			final Inode inode = HttpUtil.getJsonDirectory(json, response, "inode");
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
		} catch (final SQLException e) {
			HttpUtil.handleSqlException(response, e);
			return;
		}
	}

}
