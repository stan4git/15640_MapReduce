package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeInterface extends Remote {
	public void uploadChunk(String filename, byte[] chunk) throws RemoteException;
	public void removeFile(String filename) throws RemoteException;
	public byte[] getFile(String filename) throws RemoteException;
}