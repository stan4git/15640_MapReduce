package dfs;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import util.FileStatus;
import util.IOUtil;
import util.NodeStatus;

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
	private int nameNodeRegPort;
	private String nameNodeService;
	private int replicaNum;
	private Registry nameNodeRegistry;
	private ConcurrentHashMap<String, Integer> dataNodeAvailableSlotList;
	private ConcurrentHashMap<String, NodeStatus> dataNodeStatusList;
	private ConcurrentHashMap<String, FileStatus> fileStatusTable;
	private ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> fileDistributionTable;
	private ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> processingFileDistributionTable;
	private boolean isRunning;
	
	
	public static void main(String[] args) {
		System.out.println("Starting up name node server...");
		NameNode nameNode = new NameNode();
		System.out.println("System is running...");
		while (nameNode.isRunning) {
			
		}
		System.out.println("System is shuting down...");
	}
	
	
	public NameNode() {
		this.isRunning = true;
		dataNodeAvailableSlotList = new ConcurrentHashMap<String, Integer>();
		setDataNodeStatusList(new ConcurrentHashMap<String, NodeStatus>());
		fileStatusTable = new ConcurrentHashMap<String, FileStatus>();
		fileDistributionTable = new ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>>();
		
		System.out.println("Loading configuration data...");
		IOUtil.readConf("conf/dfs.conf", this);
		System.out.println("Configuration data loaded successfully...");
		
		NodeMonitor nodeMonitor = new NodeMonitor(this);
		Thread monitoring = new Thread(nodeMonitor);
		monitoring.start();
		System.out.println("Start monitoring...");
		
		try {
			this.nameNodeRegistry = LocateRegistry.createRegistry(this.nameNodeRegPort);
			this.nameNodeRegistry.rebind(nameNodeService, this);
			System.out.println("Server has been set up...");
		} catch (RemoteException e) {
			e.printStackTrace();
		} 
	}

	
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(
			ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> failureList) {
		
		for (Entry<String, Hashtable<Integer, HashSet<String>>> fileTuple : failureList.entrySet()) {
			String filename = fileTuple.getKey();
			for (Entry<Integer, HashSet<String>> chunkTuple : fileTuple.getValue().entrySet()) {
				int chunkNum = chunkTuple.getKey();
				HashSet<String> newNodeList = new HashSet<String>();
				for (int i = 0; i < chunkTuple.getValue().size(); i++) {
					try {
						newNodeList.add(getMostAvailableSlotDataNode(chunkTuple.getValue()));
					} catch (RemoteException e) {
						e.printStackTrace();
						return null;
					}
				}
				failureList.get(filename).put(chunkNum, newNodeList);
			}
		}
		return failureList;
	}
	
	
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(
			String filename, int chunkAmount) throws RemoteException {
		if (this.fileStatusTable.contains(filename)) {
			throw new RemoteException("File name exist. Please try another.");
		} else {
			this.fileStatusTable.put(filename, FileStatus.INPROGRESS);		//updated file status table to avoid duplicated file name
		}
		
		ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> resultTable = 
				new ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>>();
		Hashtable<Integer, HashSet<String>> chunkDispatchTable = new Hashtable<Integer, HashSet<String>>();
		
		
		for (int currentChunk = 0; currentChunk < chunkAmount; currentChunk++) {
			int replicaCount = this.replicaNum;
			HashSet<String> nodeList = new HashSet<String>();
			while (replicaCount > 0) {
				if (!chunkDispatchTable.contains(currentChunk)) {
					try {
						nodeList.add(getMostAvailableSlotDataNode(nodeList));
					} catch (RemoteException e) {
						e.printStackTrace();
						return null;
					}
					chunkDispatchTable.put(currentChunk, nodeList);
				} else {
					String selectedDataNode = null;
					do {
						try {
							selectedDataNode = getMostAvailableSlotDataNode(nodeList);
						} catch (RemoteException e) {
							e.printStackTrace();
							return null;
						}
					} while (chunkDispatchTable.get(currentChunk).contains(selectedDataNode));
					chunkDispatchTable.get(currentChunk).add(selectedDataNode);
				}
				replicaCount--;
			}
		}
		
		resultTable.put(filename, chunkDispatchTable);
		this.processingFileDistributionTable.put(filename, chunkDispatchTable);
		
		System.out.println("Sending out chunk distribution list...");
		return resultTable;
	}
	

	public String getMostAvailableSlotDataNode(HashSet<String> excludeList) throws RemoteException {
		String minLoadDataNode = null;
		int maxAvailableSlots = Integer.MIN_VALUE;
		for (Entry<String, Integer> dataNode : this.dataNodeAvailableSlotList.entrySet()) {
			if (!excludeList.contains(dataNode.getValue()) && dataNode.getValue() > maxAvailableSlots) {
				minLoadDataNode = dataNode.getKey();
				maxAvailableSlots = dataNode.getValue();
			}
		}
		if (minLoadDataNode == null) {
			throw new RemoteException("There is no data node available now.");
		}
		excludeList.add(minLoadDataNode);
		return minLoadDataNode;
	}
	

	public boolean fileDistributionConfirm(String filename) {
		this.fileDistributionTable.put(filename, this.processingFileDistributionTable.get(filename));
		this.processingFileDistributionTable.remove(filename);
		this.fileStatusTable.put(filename, FileStatus.SUCCESS);
		System.out.println(filename + "has been uploaded to DFS.");
		return true;
	}
	

	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable(
			String filename) {
		return this.fileDistributionTable;
	}

	
	public void removeChunkFromFileDistributionTable(String filename, int chunkNum, String dataNodeIP) {
		this.fileDistributionTable.get(filename).get(chunkNum).remove(dataNodeIP);
		System.out.println(dataNodeIP + " has been successfully removed from file distribution table...");
		return;
	}

	
	public void updateFileDistributionTable(
			ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> tableToBeUpdated) {
		for (Entry<String, Hashtable<Integer, HashSet<String>>> fileTuple : tableToBeUpdated.entrySet()) {
			String filename = fileTuple.getKey();
			if (!this.fileDistributionTable.contains(filename)) {
				fileDistributionTable.put(filename, fileTuple.getValue());
				fileStatusTable.put(filename, FileStatus.INPROGRESS);
			} else {
				for (Entry<Integer, HashSet<String>> chunkTuple : fileTuple.getValue().entrySet()) {
					int chunkNum = chunkTuple.getKey();
					if (!fileDistributionTable.get(filename).contains(chunkNum)) {
						fileDistributionTable.get(filename).put(chunkNum,chunkTuple.getValue());
					} else {
						fileDistributionTable.get(filename).get(chunkNum).addAll(chunkTuple.getValue());
					}
				}
			}
		}
		System.out.println("File distribution table has been successfully updated...");
		return;
	}

	
	public void setFileUploadFinished(String filename) throws RemoteException {
		if (this.fileStatusTable.contains(filename)) {
			this.fileStatusTable.put(filename, FileStatus.SUCCESS);
		} else {
			throw new RemoteException("File not exist!!");
		}
		return;
	}
	
	
	public void registerDataNode(String dataNodeIP, int availableSlot) {
		this.dataNodeAvailableSlotList.put(dataNodeIP, availableSlot);
		System.out.println(dataNodeIP + " has been added to data node list...");
	}


	public HashSet<String> getHealthyNodes() {
		HashSet<String> returnList = new HashSet<String>();
		for (Entry<String, NodeStatus> node : this.getDataNodeStatusList().entrySet()) {
			if (node.getValue() == NodeStatus.HEALTHY) {
				returnList.add(node.getKey());
			}
		}
		return returnList;
	}


	public ConcurrentHashMap<String, NodeStatus> getDataNodeStatusList() {
		return dataNodeStatusList;
	}

	
	public void setDataNodeAvailableSlotList(
			ConcurrentHashMap<String, Integer> dataNodeAvailableSlotList) {
		this.dataNodeAvailableSlotList = dataNodeAvailableSlotList;
	}


	public void setDataNodeStatusList(
			ConcurrentHashMap<String, NodeStatus> dataNodeStatusList) {
		this.dataNodeStatusList = dataNodeStatusList;
	}


	public void setFileStatusTable(
			ConcurrentHashMap<String, FileStatus> fileStatusTable) {
		this.fileStatusTable = fileStatusTable;
	}


	public void setFileDistributionTable(
			ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> fileDistributionTable) {
		this.fileDistributionTable = fileDistributionTable;
	}

	
	public boolean fileExist(String filename) {
		return this.fileDistributionTable.contains(filename);
	}

	
	/**
	 * Return all the nodes registered in DFS.
	 * @return A map consist of each node's ip address and file chunks on it.
	 */
	public ConcurrentHashMap<String, Integer> getDataNodeAvailableSlotList() {
		return this.dataNodeAvailableSlotList;
	}
	
	
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable() {
		return this.fileDistributionTable;
	}
	
	
	/**
	 * Return all the files uploaded to DFS without path.
	 * @return A map consist of file name and file status.
	 */
	public ConcurrentHashMap<String, FileStatus> getFileStatusTable() {
		return this.fileStatusTable;
	}
	

	public void terminate() {
		this.isRunning = false;
	}
}
