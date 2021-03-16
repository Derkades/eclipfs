package eclipfs.metaserver;

import java.sql.Connection;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Database {
    
    private static HikariDataSource ds;

    static {
    	// TODO Load settings from environment variables
		final String hostname = "localhost";
		final int port = 5432;
		final String databaseName = "postgres";
		final String username = "postgres";
		final String password = "secure";
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
