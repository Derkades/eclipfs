package eclipfs.metaserver.http;

import java.sql.SQLException;

import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.util.security.Constraint;

import eclipfs.metaserver.model.User;

public class HttpSecurityManager {

	private static final String[] DASHBOARD_ROLE = { "dashboard" };
	private static final String REALM_NAME = "eclipfs dashboard";

	private final ConstraintSecurityHandler security = new ConstraintSecurityHandler();
	private final BasicAuthenticator authenticator = new BasicAuthenticator();
	private final HashLoginService service = new HashLoginService();

	public HttpSecurityManager() throws SQLException {
		this.service.setName(REALM_NAME);

		final Constraint constraint = new Constraint();
		constraint.setName(Constraint.__BASIC_AUTH);
		constraint.setRoles(new String[] { "dashboard" });
		constraint.setAuthenticate(true);

		final ConstraintMapping mapping = new ConstraintMapping();
		mapping.setConstraint(constraint);
		mapping.setPathSpec("/*");

		this.security.setAuthenticator(this.authenticator);
		this.security.setRealmName(REALM_NAME);
		this.security.setLoginService(this.service);
		this.security.addConstraintMapping(mapping);

		reloadUserStore();
	}

	@Deprecated
	public LoginService getLoginService() {
		return this.service;
	}

	public SecurityHandler getSecurityHandler() {
		return this.security;
	}

	public void reloadUserStore() throws SQLException {
		final UserStore userStore = new UserStore();
		for (final User user : User.list()) {
			userStore.addUser(user.getUsername(), user.getCredential(), DASHBOARD_ROLE);
		}
		this.service.setUserStore(userStore);
	}

}
