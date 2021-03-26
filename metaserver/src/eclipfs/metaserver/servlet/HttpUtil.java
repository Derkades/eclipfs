package eclipfs.metaserver.servlet;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Optional;

import org.apache.commons.lang.Validate;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import eclipfs.metaserver.model.Directory;
import eclipfs.metaserver.model.File;
import eclipfs.metaserver.model.Inode;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class HttpUtil {

	public static void handleSqlException(final HttpServletResponse response, final SQLException e) throws IOException {
		Validate.notNull(response);
		Validate.notNull(e);
		e.printStackTrace();
		sendServerError(response, "Database error");
	}

	public static void sendServerError(final HttpServletResponse response, final String message) throws IOException {
		Validate.notNull(response);
		Validate.notNull(message);
		response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		response.setContentType("text/plain");
		response.getWriter().println(message);
	}

	public static void sendBadRequest(final HttpServletResponse response, final String message) throws IOException {
		Validate.notNull(response);
		Validate.notNull(message);
		response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		response.setContentType("text/plain");
		response.getWriter().println(message);
	}

	public static Long getLongParameter(final HttpServletRequest request, final HttpServletResponse response, final String parameterName) throws IOException {
		Validate.notNull(request);
		Validate.notNull(response);
		Validate.notNull(parameterName);
		final String value = request.getParameter(parameterName);
		if (value == null) {
			sendBadRequest(response, "Missing parameter " + parameterName);
			return null;
		}

		try {
			return Long.valueOf(value);
		} catch (final NumberFormatException e) {
			sendBadRequest(response, "Invalid parameter (not a long) " + parameterName);
			return null;
		}
	}

	private static Inode longToInode(final HttpServletResponse response, final Long inode) throws SQLException, IOException {
		if (inode == null) {
			return null;
		}

		final Optional<Inode> optInode = Inode.byId(inode);

		if (optInode.isEmpty()) {
			ApiError.FILE_NOT_EXISTS.send(response);
		}

		return optInode.orElse(null);
	}

	private static File longToFile(final HttpServletResponse response, final Long id) throws SQLException, IOException {
		final Inode inode = longToInode(response, id);

		if (inode == null) {
			return null;
		}

		if (inode.isFile()) {
			return (File) inode;
		} else {
			ApiError.IS_A_DIRECTORY.send(response);
			return null;
		}
	}

	private static Directory longToDirectory(final HttpServletResponse response, final Long id) throws SQLException, IOException {
		final Inode inode = longToInode(response, id);

		if (inode == null) {
			return null;
		}

		if (!inode.isFile()) {
			return (Directory) inode;
		} else {
			ApiError.IS_A_FILE.send(response);
			return null;
		}
	}

	public static Inode getInodeParameter(final HttpServletRequest request, final HttpServletResponse response) throws IOException, SQLException {
		if (request.getParameter("inode") != null) {
			return longToInode(response, getLongParameter(request, response, "inode"));
		} else {
			final Directory parent = longToDirectory(response, getLongParameter(request, response, "inode_p"));
			final String name = HttpUtil.getStringParameter(request, response, "name");
			if (parent == null || name == null) {
				return null;
			}

			final Optional<Inode> opt = parent.getChild(name);
			if (opt.isEmpty()) {
				ApiError.NAME_NOT_EXISTS.send(response);
			}
			return opt.orElse(null);
		}
	}

	public static File getFileInodeParameter(final HttpServletRequest request, final HttpServletResponse response) throws IOException, SQLException {
		return longToFile(response, getLongParameter(request, response, "file"));
	}

	public static Directory getDirectoryInodeParameter(final HttpServletRequest request, final HttpServletResponse response) throws IOException, SQLException {
		return longToDirectory(response, getLongParameter(request, response, "directory"));
	}

	public static Directory getDirectoryInodeParameter(final HttpServletRequest request, final HttpServletResponse response, final String parameterName) throws IOException, SQLException {
		return longToDirectory(response, getLongParameter(request, response, parameterName));
	}

	public static String getStringParameter(final HttpServletRequest request, final HttpServletResponse response, final String parameterName) throws IOException {
		Validate.notNull(request);
		Validate.notNull(response);
		Validate.notNull(parameterName);
		final String value = request.getParameter(parameterName);
		if (value == null) {
			sendBadRequest(response, "Missing parameter " + parameterName);
			return null;
		}
		return value;
	}

	public static JsonObject readJsonFromRequestBody(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		Validate.notNull(request);
		Validate.notNull(response);
		try (JsonReader reader = new JsonReader(new InputStreamReader(request.getInputStream()))) {
			try {
				final JsonElement element = JsonParser.parseReader(reader);
				if (!element.isJsonObject()) {
					sendBadRequest(response, "Provided json is not a json object");
					return null;
				}
				return element.getAsJsonObject();
			} catch (final JsonSyntaxException e) {
				sendBadRequest(response, "Bad json syntax");
				return null;
			}
		}
	}

	public static String getJsonString(final JsonObject json, final HttpServletResponse response, final String memberName) throws IOException {
		Validate.notNull(json);
		Validate.notNull(memberName);
		Validate.notNull(response);
		if (!json.has(memberName) ||
				!json.get(memberName).isJsonPrimitive() ||
				!json.get(memberName).getAsJsonPrimitive().isString()) {
			sendBadRequest(response, "Missing or invalid json member '" + memberName + "'");
			return null;
		} else {
			return json.get(memberName).getAsString();
		}
	}

	public static Long getJsonLong(final JsonObject json, final HttpServletResponse response, final String memberName) throws IOException {
		Validate.notNull(json);
		Validate.notNull(memberName);
		Validate.notNull(response);
		if (!json.has(memberName) ||
				!json.get(memberName).isJsonPrimitive() ||
				!json.get(memberName).getAsJsonPrimitive().isNumber()) {
			sendBadRequest(response, "Missing or invalid json member '" + memberName + "'");
			return null;
		} else {
			return json.get(memberName).getAsLong();
		}
	}

	public static long[] getJsonLongArray(final JsonObject json, final HttpServletResponse response, final String memberName) throws IOException {
		Validate.notNull(json);
		Validate.notNull(memberName);
		Validate.notNull(response);
		if (!json.has(memberName) ||
				!json.get(memberName).isJsonArray()) {
			sendBadRequest(response, "Missing or invalid json member '" + memberName + "'");
			return null;
		} else {
			final JsonArray jsonArray = json.get(memberName).getAsJsonArray();
			final long[] array = new long[jsonArray.size()];
			for (int i = 0; i < array.length; i++) {
				array[i] = jsonArray.get(i).getAsLong();
			}
			return array;
		}
	}

	public static Inode getJsonInode(final JsonObject json, final HttpServletResponse response) throws SQLException, IOException {
		if (json.has("inode")) {
			return longToInode(response, getJsonLong(json, response, "inode"));
		} else {
			final Directory parent = longToDirectory(response, getJsonLong(json, response, "inode_p"));
			final String name = getJsonString(json, response, "name");
			if (parent == null || name == null) {
				return null;
			}

			final Optional<Inode> opt = parent.getChild(name);
			if (opt.isEmpty()) {
				ApiError.NAME_NOT_EXISTS.send(response);
			}
			return opt.orElse(null);
		}
	}

	public static File getJsonFile(final JsonObject json, final HttpServletResponse response) throws SQLException, IOException {
		return getJsonFile(json, response, "file");
	}

	public static File getJsonFile(final JsonObject json, final HttpServletResponse response, final String memberName) throws SQLException, IOException {
		return longToFile(response, getJsonLong(json, response, memberName));
	}

	public static Directory getJsonDirectory(final JsonObject json, final HttpServletResponse response) throws SQLException, IOException {
		return getJsonDirectory(json, response, "directory");
	}

	public static Directory getJsonDirectory(final JsonObject json, final HttpServletResponse response, final String memberName) throws SQLException, IOException {
		return longToDirectory(response, getJsonLong(json, response, memberName));
	}

	public static long[] getJsonNumberArray(final JsonObject json, final HttpServletResponse response, final String memberName) throws IOException {
		Validate.notNull(json);
		Validate.notNull(memberName);
		Validate.notNull(response);
		if (!json.has(memberName) ||
				!json.get(memberName).isJsonArray()) {
			sendBadRequest(response, "Missing or invalid json member '" + memberName + "'");
			return null;
		} else {
			final JsonArray jsonArray = json.getAsJsonArray(memberName);
			final long[] array = new long[jsonArray.size()];
			int i = 0;
			for (final JsonElement elem : json.getAsJsonArray(memberName)) {
				if (elem.isJsonPrimitive() &&
						elem.getAsJsonPrimitive().isNumber()) {
					array[i] = elem.getAsLong();
					i++;
				} else {
					sendBadRequest(response, "Json array '" + memberName + "' must only contain integers");
					return null;
				}
			}
			return null;
		}
	}

	public static String[] getJsonStringArray(final JsonObject json, final HttpServletResponse response, final String memberName) throws IOException {
		Validate.notNull(json);
		Validate.notNull(memberName);
		Validate.notNull(response);
		if (!json.has(memberName) ||
				!json.get(memberName).isJsonArray()) {
			sendBadRequest(response, "Missing or invalid json member '" + memberName + "'");
			return null;
		} else {
			final JsonArray jsonArray = json.getAsJsonArray(memberName);
			final String[] array = new String[jsonArray.size()];
			int i = 0;
			for (final JsonElement elem : json.getAsJsonArray(memberName)) {
				if (elem.isJsonPrimitive() &&
						elem.getAsJsonPrimitive().isString()) {
					array[i] = elem.getAsString();
					i++;
				} else {
					sendBadRequest(response, "Json array '" + memberName + "' must only contain integers");
					return null;
				}
			}
			return null;
		}
	}

	public static URL getJsonAddress(final JsonObject json, final HttpServletResponse response, final String memberName) throws IOException {
		Validate.notNull(json);
		Validate.notNull(memberName);
		Validate.notNull(response);
		if (!json.has(memberName) ||
				!json.get(memberName).isJsonPrimitive() ||
				!json.get(memberName).getAsJsonPrimitive().isString()) {
			sendBadRequest(response, "Missing or invalid json member '" + memberName + "'");
			return null;
		} else {
			try {
				return new URL(json.get(memberName).getAsString());
			} catch (final MalformedURLException e) {
				return null;
			}
		}
	}

	public static void writeJsonResponse(final JsonObject jsonResponse, final HttpServletResponse response, final String memberName) throws IOException {
		Validate.notNull(jsonResponse);
		Validate.notNull(response);
		response.setContentType("application/json");
		response.getWriter().write(jsonResponse.toString());
	}

	public static JsonWriter getJsonWriter(final HttpServletResponse response) throws IOException {
		Validate.notNull(response);
		response.setContentType("application/json");
		return new JsonWriter(response.getWriter());
	}

	public static void writeSuccessTrueJson(final HttpServletResponse response) throws IOException {
		Validate.notNull(response);
		try (JsonWriter writer = getJsonWriter(response)) {
			writer.beginObject().name("success").value(true).endObject();
		}
	}

}
