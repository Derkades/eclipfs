package eclipfs.metaserver.http;

import java.util.Objects;

import org.eclipse.jetty.util.security.Credential;
import org.eclipse.jetty.util.security.Password;

import eclipfs.metaserver.MetaServer;

public class BcryptCredential extends Credential {

	private static final long serialVersionUID = 1L;

	private final String hash;

	public BcryptCredential(final String hash) {
		this.hash = hash;
	}

	@Override
	public boolean check(Object credentials) {
        if (credentials instanceof char[]) {
			credentials = new String((char[]) credentials);
		}

        if (credentials instanceof Password || credentials instanceof String) {
        	return MetaServer.PASSWORD_ENCODER.matches(credentials.toString(), this.hash);
        }

        if (credentials instanceof BcryptCredential){
        	return equals(credentials);
        }

        throw new IllegalStateException(credentials.toString());
	}

	@Override
	public boolean equals(final Object other) {
		return other != null && other instanceof BcryptCredential && Objects.equals(this.hash, ((BcryptCredential) other).hash);
	}

}
