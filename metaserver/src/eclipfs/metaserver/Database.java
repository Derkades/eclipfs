package eclipfs.metaserver;

import java.sql.Connection;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Database {

    private static HikariDataSource ds;

    static {
    	try {
    		// required to make it work with relocation
			Class.forName("org.postgresql.Driver");
		} catch (final ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		final String hostname = System.getenv("POSTGRES_HOSTNAME");
		final int port = Integer.parseInt(System.getenv("POSTGRES_PORT"));
		final String databaseName = System.getenv("POSTGRES_DB");
		final String username = System.getenv("POSTGRES_USER");
		final String password = System.getenv("POSTGRES_PASSWORD");
		final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:postgresql://%s:%s/%s", hostname, port, databaseName));
        config.setUsername(username);
        config.setPassword(password);
        ds = new HikariDataSource(config);
    }

	public static Connection getConnection() throws SQLException {
		return ds.getConnection();
	}

}
