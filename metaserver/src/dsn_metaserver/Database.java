package dsn_metaserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Database {
	
	// TODO Load settings from environment variables
	
	// TODO make private
	public static Connection getConnection() throws SQLException {
//		MetaServer.LOGGER.info("new database connection");
		final String hostname = "localhost";
		final int port = 5432;
		final String databaseName = "postgres";
		final String username = "postgres";
		final String password = "secure";
		
		return DriverManager.getConnection(String.format("jdbc:postgresql://%s:%s/%s", hostname, port, databaseName),
				username, password);
	}
	
	// TODO some sort of connection pool system
	
	private static Connection connection = null;
	
	public static PreparedStatement prepareStatement(final String sql) throws SQLException {
		if (connection == null || connection.isClosed()) {
			connection = getConnection();
		}
		
		return connection.prepareStatement(sql);
	}

}
