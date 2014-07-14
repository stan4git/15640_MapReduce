package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public interface DataNodeInterface extends Remote {
	/** 
	 * Used to upload chunks from other data nodes or clients.
	 * @param filename String The name of file to be uploaded.
	 * @param chunk	byte[] The content of file chunk.
	 * @param chunkNum Integer The number of chunk to be uploaded.
	 * @param fromIP String The ip address where this file chunk is uploaded from.
	 * @throws RemoteException
	 */
	public void uploadChunk(String filename, byte[] chunk, int chunkNum, String fromIP) throws RemoteException;
	
	/**
	 * Remove a specific chunk of a file from this data node. 
	 * @param filename String The name of the file.
	 * @param chunkNum Integer The chunk number of file to be deleted.
	 * @throws RemoteException
	 */
	public void removeChunk(String filename, int chunkNum) throws RemoteException;
	
	/**
	 * Fetch a chunk of file.
	 * @param filename String The name of the file.
	 * @param chunkNum Integer The chunk number of file to be fetched.
	 * @return byte[] The content of this file chunk.
	 * @throws RemoteException
	 */
	public byte[] getFile(String filename, int chunkNum) throws RemoteException;
	
	/**
	 * Heartbeat method to check if data node is alive.
	 * @return True
	 * @throws RemoteException
	 */
	public boolean heartbeat() throws RemoteException;
	
	/**
	 * Check if a specific chunk of file is on this data node.
	 * @param filename String The name of the file.
	 * @param chunkNum Integer The chunk number of this file.
	 * @return True if exist. False if not.
	 * @throws RemoteException
	 */
	public boolean hasChunk(String filename, int chunkNum) throws RemoteException;
	
	/**
	 * Download a file chunk from another data node.
	 * @param filename String The name of the file.
	 * @param chunkNum Integer The chunk number of this file to be download.
	 * @param fromIP String The IP address to download file chunk from.
	 * @throws RemoteException
	 */
	public void downloadChunk(String filename, int chunkNum, String fromIP) throws RemoteException;
	
	/**
	 * Return current available slots.
	 * @return int Current available slots.
	 * @throws RemoteException
	 */
	public int getAvailableChunkSlot() throws RemoteException;
	
	/**
	 * Return chunk lists of files on this data node.
	 * @return the chunk list of file.
	 * @throws RemoteException
	 */
	public ConcurrentHashMap<String, HashSet<Integer>> getFileChunkList() throws RemoteException;
	
	/**
	 * Terminate this data node.
	 * @throws RemoteException
	 */
	public void terminate() throws RemoteException;
}