package eclipfs.metaserver.http;

import java.util.HashMap;
import java.util.Map;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

public class PasswordChecker {

	private final PasswordEncoder passwordEncoder = new BCryptPasswordEncoder();
	private final Map<String, Boolean> cache = new HashMap<>();

	public PasswordChecker() {

	}

	public boolean checkPassword(final String password, final String hash) {
		if (this.cache.containsKey(password + hash)) {
			return this.cache.get(password + hash);
		} else {
			final boolean matches = this.passwordEncoder.matches(password, hash);
			if (this.cache.size() > 1000) {
				this.cache.clear();
			}
			this.cache.put(password + hash, matches);
			return matches;
		}
	}

	public String hashPassword(final String password) {
		return this.passwordEncoder.encode(password);
	}

}
