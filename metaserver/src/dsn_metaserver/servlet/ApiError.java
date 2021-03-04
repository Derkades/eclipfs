package dsn_metaserver.servlet;

import java.io.IOException;

import com.google.gson.stream.JsonWriter;

import jakarta.servlet.ServletResponse;

public enum ApiError {

	DIRECTORY_NOT_EXISTS(1),
	FILE_NOT_EXISTS(2),
//	DIRECT_UPLOAD_FILE_DISTRIBUTED(3),
	DIRECT_UPLOAD_FILE_INVALID_SIZE(4),
	FILE_ALREADY_UPLOADED(5),
	DIRECTORY_ALREADY_EXISTS(6),
	INVALID_FILE_DIRECTORY_NAME(7),
//	MISSING_READ_ACCESS(8),
	MISSING_WRITE_ACCESS(9),
	DIRECTORY_NOT_EMPTY(10),
	TEMPORARY_NODE_SHORTAGE(11),
	DESTINATION_EXISTS(12),
	IS_DELETED(13),
	NODE_ADDRESS_UNREACHABLE(14),
	CHUNK_NOT_EXISTS(15),
	ORDER_NOT_EXISTS(16),
	FILE_DOWNLOAD_NODES_UNAVAILABLE(17),
	IS_A_DIRECTORY(18),
	FILE_ALREADY_EXISTS(19),
	CHUNK_ALREADY_EXISTS(20),
	SIZE_MISMATCH(21),
	
	;
	
	private int errorCode;
	private String description;
	
	ApiError(final int errorCode) {
		this.errorCode = errorCode;
		this.description = null;
	}
	
	ApiError(final int errorCode, final String description) {
		this.errorCode = errorCode;
		this.description = description;
	}
	
	public int getErrorCode() {
		return this.errorCode;
	}
	
	public void send(final ServletResponse response) throws IOException {
		send(response, null);
	}
	
	public void send(final ServletResponse response, final String info) throws IOException {
		response.setContentType("application/json");
		try (final JsonWriter writer = new JsonWriter(response.getWriter())) {
			writer.beginObject();
			writer.name("error");
			writer.value(this.errorCode);
			writer.name("error_message");
			if (info == null) {
				writer.value(toString());
			} else {
				writer.value(toString() + " - " + info);
			}
			writer.endObject();
		}
	}
	
	@Override
	public String toString() {
		return this.description == null ? this.name() : this.description;
	}
	
}
