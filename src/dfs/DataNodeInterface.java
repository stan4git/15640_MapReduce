package dfs;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public interface DataNodeInterface extends Remote, Serializable {
	public void uploadChunk(String filename, byte[] chunk, int chunkNum, String fromIP) throws RemoteException;
	public void removeChunk(String filename, int chunkNum) throws RemoteException;
	public byte[] getFile(String filename, int chunkNum) throws RemoteException;
	public boolean heartbeat() throws RemoteException;
	public boolean hasChunk(String filename, int chunkNum) throws RemoteException;
	public void downloadChunk(String filename, int chunkNum, String fromIP) throws RemoteException;
	public int getAvailableChunkSlot() throws RemoteException;
	public ConcurrentHashMap<String, HashSet<Integer>> getFileChunkList() throws RemoteException;
	public void terminate() throws RemoteException;
}