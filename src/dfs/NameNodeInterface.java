package dfs;

import java.rmi.Remote;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import util.FileStatus;

public interface NameNodeInterface extends Remote {
	public Map<String, FileStatus> getFileList();
	public Map<String, Set<String>> getNodeList();
	public Map<Integer, Integer> getChunkDistributionList();
	public Map<String, Map<Integer, Set<String>>> generateChunkDistributionList();
}
