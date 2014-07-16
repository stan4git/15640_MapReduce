package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

import util.NodeStatus;
import util.FileStatus;

public interface NameNodeInterface extends Remote {
	/**
	 * Get file status on DFS.
	 * @return ConcurrentHashMap<String, FileStatus> The file status table.
	 * @throws RemoteException
	 */
	public ConcurrentHashMap<String, FileStatus> getFileStatusTable() throws RemoteException;
	
	/**
	 * Get available slots table for all data nodes.
	 * @return ConcurrentHashMap<String, Integer> A table of data nodes available slots.
	 * @throws RemoteException
	 */
	public ConcurrentHashMap<String, Integer> getDataNodeAvailableSlotList() throws RemoteException;
	
	/**
	 * Get the table of data nodes status.
	 * @return ConcurrentHashMap<String, NodeStatus> A table of data nodes status.
	 * @throws RemoteException
	 */
	public ConcurrentHashMap<String, NodeStatus> getDataNodeStatusList() throws RemoteException;
	
	/**
	 * Get a table of file distribution on DFS.
	 * @return ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> A file contains all file distribution on DFS.
	 * @throws RemoteException
	 */
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable() throws RemoteException;
//	public ConcurrentHashMap<String, ConcurrentHashMap<Integer, HashSet<String>>> getFileDistributionTable(String filename) throws RemoteException;
	
	/**
	 * Generate a distribution guidance table.
	 * @param filename String The name of file to be distributed.
	 * @param chunkAmount Integer The amount of chunks to be distributed.
	 * @return ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> A table that contains distribution guidance.
	 * @throws RemoteException
	 */
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(
			String filename, int chunkAmount) throws RemoteException;
	
	/**
	 * Generate distribution guidance for those failed uploading.
	 * @param failureList ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> A table contains all the failed uploading operations.
	 * @return ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> A new distribution guidance.
	 * @throws RemoteException
	 */
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(
			ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> failureList) throws RemoteException;

	
	/**
	 * Used by client to acknowledge name node after uploading all the chunks. Then the name node will update correspondence tables.
	 * @param filename String The name of file uploaded.
	 * @return boolean Result of acknowledge.
	 * @throws RemoteException
	 */
	public boolean fileDistributionConfirm(String filename) throws RemoteException;
	
//	public void updateFileDistributionTable(ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> tableToBeUpdated) throws RemoteException;
	
	/**
	 * Remove a chunk from file distribution table.
	 * @param filename The name of file to be removed.
	 * @param chunkNum The chunk number of file to be removed.
	 * @param dataNodeIP The IP address of data node where this chunk is located.
	 * @throws RemoteException
	 */
	public void removeChunkFromFileDistributionTable(String filename, int chunkNum, String dataNodeIP) throws RemoteException;
	
	/**
	 * Register a data node.
	 * @param dataNodeIP String The IP address of data node.
	 * @param availableSlot Integer Available slots on this data node.
	 * @throws RemoteException
	 */
	public void registerDataNode(String dataNodeIP, int availableSlot) throws RemoteException;
	
//	public void chunkCopyMadeConfirm(String filename, int chunkNum, String fromIP) throws RemoteException;
	/**
	 * Get a list of healthy data nodes.
	 * @return HashSet<String> A table of data nodes.
	 * @throws RemoteException
	 */
	public HashSet<String> getHealthyNodes() throws RemoteException;
	
	/**
	 * Check if a specific file exists on DFS.
	 * @param filename The file name to be checked.
	 * @return True if exists. 
	 * @throws RemoteException
	 */
	public boolean fileExist(String filename) throws RemoteException;
	
	/**
	 * Terminate this name node.
	 * @throws RemoteException
	 */
	public void terminate() throws RemoteException;
	/**
	 * This method is used to update the node status
	 * @param status
	 * @param node
	 * @throws RemoteException
	 */
	public void setNodeStatus(String node, NodeStatus status) throws RemoteException;
}
