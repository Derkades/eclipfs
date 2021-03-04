package dsn_metaserver.servlet;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;

import org.apache.commons.lang.Validate;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

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
	
	// TODO swap memberName and response for json functions to be consistent with parameter functions
	
	public static String getJsonString(final JsonObject json, final String memberName, final HttpServletResponse response) throws IOException {
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
	
	public static Long getJsonLong(final JsonObject json, final String memberName, final HttpServletResponse response) throws IOException {
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
	
	public static long[] getJsonNumberArray(final JsonObject json, final String memberName, final HttpServletResponse response) throws IOException {
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
	
	public static String[] getJsonStringArray(final JsonObject json, final String memberName, final HttpServletResponse response) throws IOException {
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
	
	public static URL getJsonAddress(final JsonObject json, final String memberName, final HttpServletResponse response) throws IOException {
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
	
	public static void writeJsonResponse(final JsonObject jsonResponse, final HttpServletResponse response) throws IOException {
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
		try (JsonWriter writer = getJsonWriter(response)) {
			writer.beginObject().name("success").value(true).endObject();
		}
	}

}
