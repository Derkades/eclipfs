package dsn_metaserver.command;

import java.sql.SQLException;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import dsn_metaserver.MetaServer;
import dsn_metaserver.exception.NotExistsException;
import dsn_metaserver.model.Directory;

public class ChangeDirectoryCommand extends Command {

	@Override
	public void run(final String[] args) throws SQLException, NotExistsException {
		if (args.length != 1) {
			System.out.println("<path>");
		}
		
		String path = args[0];
		if (!path.startsWith("/")) {
			if (MetaServer.WORKING_DIRECTORY.isPresent()) {
				path = MetaServer.WORKING_DIRECTORY.get().getAboslutePath() + path;
			} else {
				path = "/" + path;
			}
		}
		path = StringUtils.removeEnd(path, "/");
		final Optional<Directory> directory = Directory.findByPath(path);
		if (directory.isEmpty()) {
			System.out.println("This directory does not exist");
		} else {
			MetaServer.WORKING_DIRECTORY = directory;
		}
	}

}
