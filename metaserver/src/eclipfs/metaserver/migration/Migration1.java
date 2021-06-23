package eclipfs.metaserver.migration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;

public class Migration1 extends Migration {

	@Override
	void runMigration(final Logger logger, final Connection connection) throws SQLException {
		logger.info("Adding size column to inode");
		try (PreparedStatement query = connection.prepareStatement("ALTER TABLE \"inode\" ADD COLUMN \"size\" bigint")) {
			query.execute();
		}
		logger.info("Set size to 0 for all directory inodes");
		try (PreparedStatement query = connection.prepareStatement("UPDATE inode SET size = 0 WHERE \"is_file\" = 'f'")) {
			query.execute();
		}
		logger.info("Setting size for all file inodes");
		try (PreparedStatement query = connection.prepareStatement("SELECT \"id\" FROM inode WHERE \"is_file\" = 't'")) {
			query.execute();
			final ResultSet result = query.executeQuery();
			while (result.next()) {
				final int inode = result.getInt(1);
				logger.info("Setting size for inode {}", inode);
				try (PreparedStatement update = connection.prepareStatement("UPDATE inode SET \"size\" = (SELECT SUM(size) FROM \"chunk\" WHERE file=?) WHERE \"id\"=?")) {
					update.setInt(1, inode);
					update.setInt(2, inode);
					update.execute();
				}
			}
		}
		logger.info("Set size to 0 for all remaining inodes (empty files)");
		try (PreparedStatement query = connection.prepareStatement("UPDATE inode SET size = 0 WHERE \"is_file\" = 't' AND size IS NULL")) {
			query.execute();
		}
		logger.info("Make size column NOT NULL ");
		try (PreparedStatement query = connection.prepareStatement("ALTER TABLE \"inode\" ALTER \"size\" SET NOT NULL")) {
			query.execute();
		}
		logger.info("Remove chunk size column");
		try (PreparedStatement query = connection.prepareStatement("ALTER TABLE chunk DROP size")) {
			query.execute();
		}
		logger.info("Removing writing chunk size column");
		try (PreparedStatement query = connection.prepareStatement("ALTER TABLE chunk_writing DROP size")) {
			query.execute();
		}
	}

}
