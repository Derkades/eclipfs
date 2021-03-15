package eclipfs.metaserver.command;

import eclipfs.metaserver.model.User;

public class UserAddCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("Usage: useradd <user>");
			return;
		}
		final User user = User.create(args[0]);
		user.setWriteAccess(true);
		System.out.println("User " + args[0] + " created.");
	}

}
