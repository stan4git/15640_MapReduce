package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DFSClientInterface extends Remote {
	public void sendChunkReceivedACK(String fromIP, String filename, int chunkNum) throws RemoteException;
}