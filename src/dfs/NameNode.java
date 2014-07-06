package dfs;

import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import util.FileStatus;
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
	private int clientRegPort;
	private int clientPort;
	private String clientServiceName;
	private int maxChunkSlot;
	private int maxChunkSize;
	private String nameNodeIP;
	private int nameNodeRegPort;
	private int nameNodePort;
	private String nameNodeService;
	private int dataNodeRegPort;
	private int dataNodePort;
	private String dataNodeService;
	private int replicaNum;
	private int heartbeatCheckThreshold;
	private int heartbeatInterval;
	private String dataNodePath;
	private String checkPointPath;
	private int chunkTranferRetryThreshold;
	private int ackTimeout;
	private Registry nameNodeRegistry;
	private NameNodeInterface nameNode;
	private ConcurrentHashMap<String, Integer> dataNodeAvailableSlotList;
	private ConcurrentHashMap<String, NodeStatus> dataNodeStatusList;
	private ConcurrentHashMap<String, FileStatus> fileStatusTable;
	private ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> fileDistributionTable;
	private boolean isRunning;
	
	
	public static void main(String[] args) {
		NameNode nameNode = new NameNode();
		
		
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
	
	
	/**
	 * Return all the files uploaded to DFS without path.
	 * @return A map consist of file name and file status.
	 */
	@Override
	public ConcurrentHashMap<String, FileStatus> getFileStatusTable() {
		return this.fileStatusTable;
	}

	
	/**
	 * Return all the nodes registered in DFS.
	 * @return A map consist of each node's ip address and file chunks on it.
	 */
	@Override
	public ConcurrentHashMap<String, Integer> getDataNodeAvailableSlotList() {
		return this.dataNodeAvailableSlotList;
	}
	
	
	@Override
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable() {
		return this.fileDistributionTable;
	}

	
	@Override
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(
			String filename, int chunkAmount) {
		ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> resultTable = 
				new ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>>();
		Hashtable<Integer, HashSet<String>> chunkDispatchTable = new Hashtable<Integer, HashSet<String>>();
		for (int currentChunk = 0; currentChunk < chunkAmount; currentChunk++) {
			if (!chunkDispatchTable.contains(currentChunk)) {
				HashSet<String> dnl = new HashSet<String>();
				dnl.add(getMostAvailableSlotDataNode());
				chunkDispatchTable.put(currentChunk, dnl);
			} else {
				chunkDispatchTable.get(currentChunk).add(getMostAvailableSlotDataNode());
			}
		}
		resultTable.put(filename, chunkDispatchTable);
		System.out.println("Senging out chunk distribution list...");
		return resultTable;
	}
	

	public String getMostAvailableSlotDataNode() {
		String minLoadDataNode = null;
		int maxAvailableSlots = Integer.MIN_VALUE;
		for (Entry<String, Integer> dataNode : this.dataNodeAvailableSlotList.entrySet()) {
			if (dataNode.getValue() > maxAvailableSlots) {
				minLoadDataNode = dataNode.getKey();
				maxAvailableSlots = dataNode.getValue();
			}
		}
		return minLoadDataNode;
	}
	

	@Override
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable(
			String filename) {
		return this.fileDistributionTable;
	}

	
	@Override
	public void removeChunkFromFileDistributionTable(String filename, int chunkNum, String dataNodeIP) {
		this.fileDistributionTable.get(filename).get(chunkNum).remove(dataNodeIP);
		System.out.println(dataNodeIP + " has been successfully removed from file distribution table...");
		return;
	}

	
	@Override
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(
			ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> failureList) {
		ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> newList = 
				new ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>>();
		
		
		return null;
	}

	
	@Override
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
	
	
	@Override
	public void registerDataNode(String dataNodeIP, int availableSlot) {
		this.dataNodeAvailableSlotList.put(dataNodeIP, availableSlot);
		System.out.println(dataNodeIP + " has been added to data node list...");
	}


	@Override
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


	@Override
	public void termiate() {
		this.isRunning = false;
	}

}
