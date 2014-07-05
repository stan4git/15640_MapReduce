package dfs;

import java.rmi.Naming;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import util.FileStatus;

/**
 * 1. node list - from conf
 * 2. node status monitoring (heart beat, RMI call)
 * 3. load replica number from configuration file
 * 4. hashmap<file name : hashmap<chunk num : hashset<node list>>>
 * 5. dfsScheduler (node picking, checkpoint) -- also as stub for client invocation
 * 6. connection mapping
 * 7. registry server
 * 8. hashmap<node : hashSet<file list>>
 * 9. file list
 */
public class NameNode implements NameNodeInterface {
	private static Registry registryServer;
	
	
	public static void main(String[] args) {
		NameNode nameNode = new NameNode();
		nameNode.init();
		
	}
	
	private void init() {
		
	}
	
	/**
	 * Return all the files uploaded to DFS without path.
	 * @return A map consist of file name and file status.
	 */
	@Override
	public ConcurrentHashMap<String, FileStatus> getFullFileStatusList() {
		return null;
	}

	/**
	 * Return all the nodes registered in DFS.
	 * @return A map consist of each node's ip address and file chunks on it.
	 */
	@Override
	public ConcurrentHashMap<String, HashSet<String>> getFullNodeList() {
		return null;
	}
	
	
	@Override
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable() {
		return null;
	}

	@Override
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(
			String filename, int chunkAmount) {
		return null;
	}
	
	public void monitorDataNode() {
		
	}

	@Override
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(
			ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> failureList) {
		
		return null;
	}

	@Override
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable(
			String filename) {
		// TODO Auto-generated method stub
		return null;
	}
}
