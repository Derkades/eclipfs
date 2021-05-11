package eclipfs.metaserver;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Database {

	private static final Logger LOGGER = LoggerFactory.getLogger("Database");

	private static HikariDataSource ds;

	static {
		try {
			// required to make it work with relocation
			Class.forName("org.postgresql.Driver");
		} catch (final ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		final HikariConfig config = new HikariConfig();
		config.setJdbcUrl(String.format("jdbc:postgresql://%s:%s/%s", getHostname(), getPort(), getDatabaseName()));
		config.setUsername(getUser());
		final String password = System.getenv("POSTGRES_PASSWORD");
		if (password != null) {
			config.setPassword(password);
		}
		ds = new HikariDataSource(config);
	}

	private static String getHostname() {
		final String hostname = System.getenv("POSTGRES_HOSTNAME");
		if (hostname == null) {
			LOGGER.warn("POSTGRES_HOSTNAME not specified, using 127.0.0.1");
			return "127.0.0.1";
		} else {
			return hostname;
		}
	}

	private static int getPort() {
		final String portString = System.getenv("POSTGRES_PORT");
		if (portString == null) {
			LOGGER.warn("POSTGRES_PORT not specified, using 5432");
			return 5432;
		} else {
			return Integer.parseInt(portString);
		}
	}

	private static String getDatabaseName() {
		final String db = System.getenv("POSTGRES_DB");
		if (db == null) {
			LOGGER.warn("POSTGRES_DB not specified, using 'postgres'");
			return "postgres";
		} else {
			return db;
		}
	}

	private static String getUser() {
		final String user = System.getenv("POSTGRES_USER");
		if (user == null) {
			LOGGER.warn("POSTGRES_USER not specified, using 'postgres'");
			return "postgres";
		} else {
			return user;
		}
	}

	public static Connection getConnection() throws SQLException {
		return ds.getConnection();
	}

}
