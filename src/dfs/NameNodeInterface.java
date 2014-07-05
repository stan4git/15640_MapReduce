package dfs;

import java.rmi.Remote;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import util.FileStatus;

public interface NameNodeInterface extends Remote {
	public ConcurrentHashMap<String, FileStatus> getFullFileStatusList();
	public ConcurrentHashMap<String, HashSet<String>> getFullNodeList();
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable();
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(String filename, int chunkAmount);
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> failureList);
}
