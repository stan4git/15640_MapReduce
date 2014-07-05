package dfs;

import java.rmi.Remote;

public interface DFSClientInterface extends Remote {
	public void receivedACK(String fromIP, String filename, String chunkNum);
}