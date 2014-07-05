package dfs;

import java.rmi.Remote;

public interface DFSClientInterface extends Remote {
	public void poke(String fromIP, String filename, String chunkNum);
}
