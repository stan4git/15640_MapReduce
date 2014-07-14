package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
/**
 * This interface provides remote services of client server.
 */
public interface DFSClientInterface extends Remote {
	/**
	 * Used by data node when chunk of file is received.
	 * @param fromIP String The IP address of the data node.
	 * @param filename String The name of file.
	 * @param chunkNum Integer The number of chunk of this file uploaded.
	 * @throws RemoteException
	 */
	public void sendChunkReceivedACK(String fromIP, String filename, int chunkNum) throws RemoteException;
}