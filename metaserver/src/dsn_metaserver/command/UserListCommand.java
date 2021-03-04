package dsn_metaserver.command;

import java.util.List;

import dnl.utils.text.table.TextTable;
import dsn_metaserver.model.User;

public class UserListCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		final List<User> users = User.list();
		
		if (users.isEmpty()) {
			System.out.println("No users");
		} else {
			final String[] columns = {"id", "username", "has password", "write access"};
			final Object[][] data = new Object[users.size()][columns.length];
			
			for (int i = 0; i < users.size(); i++) {
				final User user = users.get(i);
				data[i][0] = user.getId();
				data[i][1] = user.getUsername();
				data[i][2] = user.hasPassword();
				data[i][3] = user.hasWriteAccess();
//				data[i][3] = user.getCountQuota().isPresent() ? user.getCountQuota().get() : "N/A";
//				data[i][4] = user.getSizeQuota().isPresent() ? user.getSizeQuota().get() : "N/A";
			}
			
			new TextTable(columns, data).printTable();
		}
	}

}
