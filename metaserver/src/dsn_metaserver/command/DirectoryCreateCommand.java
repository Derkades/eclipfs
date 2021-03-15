package dsn_metaserver.command;

import java.sql.SQLException;

import dsn_metaserver.MetaServer;
import dsn_metaserver.exception.AlreadyExistsException;

public class DirectoryCreateCommand extends Command {

	@Override
	public void run(final String[] args) throws SQLException, AlreadyExistsException {
		if (args.length != 1) {
			System.out.println("<path>");
			return;
		}
		
		try {
			MetaServer.WORKING_DIRECTORY.createDirectory(args[0]);
			System.out.println("Directory created");
		} catch (final AlreadyExistsException e) {
			System.out.println(e.getMessage());
		}
	}

}
