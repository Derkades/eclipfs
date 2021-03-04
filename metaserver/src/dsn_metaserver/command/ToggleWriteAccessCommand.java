package dsn_metaserver.command;

import java.util.Optional;

import dsn_metaserver.model.User;

public class ToggleWriteAccessCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("Usage: togglewriteaccess <user>");
			return;
		}
		final Optional<User> optUser = User.get(args[0]);
		if (optUser.isPresent()) {
			final User user = optUser.get();
			final boolean bool = !user.hasWriteAccess();
			user.setWriteAccess(bool);
			System.out.println(bool ? "Enabled" : "Disabled" + " write access for " + args[0]);
		} else {
			System.out.println("User not found");
		}
	}

}
