package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeInterface extends Remote {
	public void uploadChunk(String filename, byte[] chunk, int chunkNum, String fromIP) throws RemoteException;
	public void removeFile(String filename, int chunkNum) throws RemoteException;
	public byte[] getFile(String filename, int chunkNum) throws RemoteException;
	public boolean heartbeat() throws RemoteException;
}