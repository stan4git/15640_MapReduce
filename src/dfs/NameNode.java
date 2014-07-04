package dfs;

import java.rmi.Naming;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

import util.FileStatus;

/**
 * 1. node list - from conf
 * 2. node status monitoring (heart beat, RMI call)
 * 3. load replica number from configuration file
 * 4. hashmap<file name : hashmap<chunk num : hashset<node list>>>
 * 5. dfsScheduler (node picking, file chopping, checkpoint) -- also as stub for client invocation
 * 6. connection mapping
 * 7. registry server
 * 8. hashmap<node : hashSet<file list>>
 * 9. file list
 */
public class NameNode {
	
	public static void main(String[] args) {
		
	}
	
	/**
	 * Return all the files uploaded to DFS without path.
	 * @return A map consist of file name and file status.
	 */
	public ConcurrentHashMap<String, FileStatus> listFile() {
		return null;
	}

	/**
	 * Return all the nodes registered in DFS.
	 * @return A map consist of each node's ip address and file chunks on it.
	 */
	public ConcurrentHashMap<String, HashSet<String>> listNode() {
		return null;
	}
	
	/**
	 * 
	 * @param input 
	 * @param output 
	 */
	public ConcurrentHashMap<Integer, Integer> assignFileChunk(String input, String output) {
		return null;
	}
	
	public ConcurrentHashMap<Integer, Integer> getFileChunkList(String file) {
		return null;
	}
	
	private void removeFile(String file) {
		
	}
	
}
