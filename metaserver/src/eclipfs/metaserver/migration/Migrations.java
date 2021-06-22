package eclipfs.metaserver.migration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eclipfs.metaserver.Database;

public class Migrations {

	private static final Migration[] MIGRATIONS = {
			new Migration1(),
	};

	private static final Logger LOGGER = LoggerFactory.getLogger("Migrations");

	public static void runMigrations() throws SQLException {
		try (Connection conn = Database.getConnection()) {
			final int version;

			try (PreparedStatement query = conn.prepareStatement("SELECT value_int FROM meta WHERE \"key\" = 'db_version'")) {
				final ResultSet result = query.executeQuery();
				result.next();
				version = result.getInt(1);
			}

			final int versionTarget = MIGRATIONS.length;

			if (version < versionTarget) {
				LOGGER.info("Database is {} version(s) out of date", versionTarget - version);
				for (int i = version; i < versionTarget; i++) {
					final Migration migration = MIGRATIONS[i];
					LOGGER.info("Running migration to version {}", i + 1);
					migration.runMigration(LOGGER, conn);
					LOGGER.info("Setting database version to {}", i + 1);
					try (PreparedStatement setVersion = conn.prepareStatement("UPDATE meta SET value_int=? WHERE key = 'db_version'")){
						setVersion.setInt(1, i + 1);
						setVersion.execute();
					}
				}
			} else {
				LOGGER.info("Database is up to date (version {})", version);
			}
		}
	}

}
