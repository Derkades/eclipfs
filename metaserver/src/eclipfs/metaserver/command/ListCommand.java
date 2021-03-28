package eclipfs.metaserver.command;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import dnl.utils.text.table.TextTable;
import eclipfs.metaserver.MetaServer;
import eclipfs.metaserver.model.Directory;
import eclipfs.metaserver.model.File;
import eclipfs.metaserver.model.Inode;
import xyz.derkades.derkutils.StringFormatUtils;

public class ListCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		final List<Directory> subdirs = MetaServer.WORKING_DIRECTORY.listDirectories();
		final List<File> files = MetaServer.WORKING_DIRECTORY.listFiles();
		final List<Inode> inodes = new ArrayList<>(subdirs.size() + files.size());
		inodes.addAll(subdirs);
		inodes.addAll(files);
		if (inodes.isEmpty()) {
			System.out.println("No files or directories");
		} else {
			printInodes(inodes);
		}
	}

	private void printInodes(final List<Inode> inodes) throws SQLException {
		final String[] columns = {"id", "type", "name", "size"};
		final Object[][] data = new Object[inodes.size()][columns.length];

		for (int i = 0; i < inodes.size(); i++) {
			final Inode inode = inodes.get(i);
			data[i][0] = inode.getId();
			data[i][1] = inode.isFile() ? "f" : "d";
			data[i][2] = inode.isFile() ? inode.getName() : inode.getName() + "/";
			data[i][3] = inode.isFile() ? StringFormatUtils.formatByteCount(inode.getSize()) : "";
		}

		new TextTable(columns, data).printTable();
	}

}
