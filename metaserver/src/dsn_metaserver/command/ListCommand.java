package dsn_metaserver.command;

import java.sql.SQLException;
import java.util.List;

import dnl.utils.text.table.TextTable;
import dsn_metaserver.MetaServer;
import dsn_metaserver.model.Directory;
import dsn_metaserver.model.File;

public class ListCommand extends Command {

	@Override
	public void run(final String[] args) throws Exception {
		final List<Directory> subdirs = MetaServer.WORKING_DIRECTORY.listDirectories();
		final List<File> files = MetaServer.WORKING_DIRECTORY.listFiles();
		if (subdirs.isEmpty() && files.isEmpty()) {
			System.out.println("No files or subdirectories");
			return;
		} else {
			if (subdirs.isEmpty()) {
				System.out.println("No subdirectories");
			} else {
				System.out.println("Subdirectories:");
				printDirectories(subdirs);
			}
			
			if (files.isEmpty()) {
				System.out.println("No files");
			} else {
				System.out.println("Files:");
				printFiles(files);
			}
		}
	}
	
	private void printFiles(final List<File> files) throws SQLException {
		final String[] columns = {"id", "name", "size"};
		final Object[][] data = new Object[files.size()][columns.length];
		
		for (int i = 0; i < files.size(); i++) {
			final File file = files.get(i);
			data[i][0] = file.getId();
			data[i][1] = file.getName();
			data[i][2] = file.getSize();
		}
		
		new TextTable(columns, data).printTable();
	}
	
	private void printDirectories(final List<Directory> directories) throws SQLException {
		final String[] columns = {"id", "name", "size (count)", "size (bytes)"};
		final Object[][] data = new Object[directories.size()][columns.length];
		
		for (int i = 0; i < directories.size(); i++) {
			final Directory dir = directories.get(i);
			data[i][0] = dir.getId();
			data[i][1] = dir.getName();
			data[i][2] = "?";
			data[i][3] = "?";
//			data[i][5] = dir.isPublicReadable();
//			data[i][6] = dir.isPublicWritable();
		}
		
		new TextTable(columns, data).printTable();
	}

}
