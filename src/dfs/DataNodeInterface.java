package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeInterface extends Remote {
	public void uploadChunk() throws RemoteException;
}
