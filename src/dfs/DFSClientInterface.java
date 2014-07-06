package dfs;

import java.rmi.Remote;

public interface DFSClientInterface extends Remote {
	public void sendACK(String fromIP, String filename, int chunkNum);
}