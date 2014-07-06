package dfs;

import java.rmi.Remote;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import util.NodeStatus;

import util.FileStatus;

public interface NameNodeInterface extends Remote {
	public ConcurrentHashMap<String, FileStatus> getFullFileStatusTable();
	public ConcurrentHashMap<String, Integer> getFullDataNodeList();
	public ConcurrentHashMap<String, NodeStatus> getFullDataNodeStatus();
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFullFileDistributionTable();
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable(String filename);
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(String filename, int chunkAmount);
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> failureList);
	public void updateFileDistributionTable(ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> tableToBeUpdated);
	public void removeChunkFromFileDistributionTable(String filename, int chunkNum, String dataNodeIP);
	public void registerDataNode(String dataNodeIP, int availableSlot);
	
	public HashSet<String> getHealthyNodes();
}
