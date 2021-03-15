package eclipfs.metaserver.command;

public abstract class Command {

	public abstract void run(String[] args) throws Exception;
	
}
