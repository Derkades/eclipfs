package eclipfs.metaserver.command;

import org.apache.commons.lang3.RandomStringUtils;

import eclipfs.metaserver.model.User;

public class UserAddCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("Usage: useradd <user>");
			return;
		}
		final String password = RandomStringUtils.randomAlphanumeric(64);
		System.out.println("password: " + password);
		final User user = User.create(args[0], password);
		user.setWriteAccess(true);
		System.out.println("User " + args[0] + " created.");
	}

}
