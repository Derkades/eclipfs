package eclipfs.metaserver;

import static org.apache.commons.lang3.Validate.isTrue;
import static org.apache.commons.lang3.Validate.notBlank;
import static org.apache.commons.lang3.Validate.notEmpty;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.lang3.Validate;

public class Validation {

	public static void validatePath(final String path) {
		notBlank(path, "path cannot be blank");
		isTrue(path.charAt(0) == '/', "path must start with /");
		isTrue(path.length() > 1, "path must be longer than 1 character");
		isTrue(path.charAt(path.length() - 1) != '/', "path cannot end with /");
		final String[] components = path.split("/");

		char previousChar = '0';
		for (final char c : path.toCharArray()) {
			Validate.isTrue(c != '/' || c != previousChar, "Path cannot contain two sequential slashes");
			previousChar = c;
		}

		// ignore first component, path starts with / so it is always blank.
		for (int i = 1; i < components.length; i++) {
			validateFileDirectoryName(components[i]);
		}
	}

	public static void validateFileDirectoryName(final String name) {
		notEmpty(name, "name cannot be empty '" + name + "'");
		isTrue(!name.contains("/"), "name cannot contain /");
	}

	public static void validateUsername(final String username) {
		notBlank(username, "username cannot be blank");
		isTrue(!username.contains(" "), "username cannot contain spaces");
	}

	public static void validateMD5(final byte[] checksum) {

	}

	public static void validateUrl(final String address) {
		try {
			new URL(address);
		} catch (final MalformedURLException e) {
			throw new IllegalArgumentException("Malformed URL: '" + address + "'", e);
		}
	}

}
