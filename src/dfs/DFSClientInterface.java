package dfs;

import java.io.Serializable;
import java.rmi.Remote;

public interface DFSClientInterface extends Remote {
	public void sendChunkReceivedACK(String fromIP, String filename, int chunkNum);
}