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
    	final String hostname =  System.getenv("POSTGRES_HOSTNAME");
    	LOGGER.warn("POSTGRES_HOSTNAME not specified, using 127.0.0.1");
    	return hostname != null ? hostname : "127.0.0.1";
    }

    private static int getPort() {
    	final String portString =  System.getenv("POSTGRES_PORT");
    	LOGGER.warn("POSTGRES_PORT not specified, using 5432");
    	return portString != null ? Integer.parseInt(portString) : 5432;
    }

    private static String getDatabaseName() {
    	final String db =  System.getenv("POSTGRES_DB");
    	LOGGER.warn("POSTGRES_DB not specified, using 'postgres'");
    	return db != null ? db : "postgres";
    }

    private static String getUser() {
    	final String user =  System.getenv("POSTGRES_USER");
    	LOGGER.warn("POSTGRES_USER not specified, using 'postgres'");
    	return user != null ? user : "postgres";
    }

	public static Connection getConnection() throws SQLException {
		return ds.getConnection();
	}

}
