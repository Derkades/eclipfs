package eclipfs.metaserver.migration;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;

public abstract class Migration {

	abstract void runMigration(Logger logger, Connection connection) throws SQLException;

}
