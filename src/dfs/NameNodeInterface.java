package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

import util.NodeStatus;
import util.FileStatus;

public interface NameNodeInterface extends Remote {
	public ConcurrentHashMap<String, FileStatus> getFileStatusTable();
	public ConcurrentHashMap<String, Integer> getDataNodeAvailableSlotList();
	public ConcurrentHashMap<String, NodeStatus> getDataNodeStatusList();
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable();
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable(String filename);
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(String filename, int chunkAmount) throws RemoteException;
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> failureList);
	public boolean fileDistributionConfirm(String filename);
	public void updateFileDistributionTable(ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> tableToBeUpdated);
	public void removeChunkFromFileDistributionTable(String filename, int chunkNum, String dataNodeIP);
	public void registerDataNode(String dataNodeIP, int availableSlot);
	
	public HashSet<String> getHealthyNodes();
	public boolean fileExist(String filename);
	
	public void terminate();
}
