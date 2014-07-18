package dfs;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import util.FileStatus;
import util.IOUtil;
import util.NodeStatus;
import util.PathConfiguration;

/**
 * This is the main thread class of name node.
 * It setups a thread to provide name node service and a monitor thread to update
 * status of all data nodes.
 */
public class NameNode extends UnicastRemoteObject implements NameNodeInterface {

	private static final long serialVersionUID = 455874693232909953L;
	private static Integer nameNodeRegPort;
	private static String nameNodeService;
	private Integer replicaNum;
	private static Registry nameNodeRegistry;
	private static Integer nameNodePort;
	private ConcurrentHashMap<String, Integer> dataNodeAvailableSlotList = new ConcurrentHashMap<String, Integer>();
	private ConcurrentHashMap<String, NodeStatus> dataNodeStatusList = new ConcurrentHashMap<String, NodeStatus>();
	private ConcurrentHashMap<String, FileStatus> fileStatusTable = new ConcurrentHashMap<String, FileStatus>();
	private ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> fileDistributionTable = new ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>>();
	private ConcurrentHashMap<String, Hashtable<String, HashSet<Integer>>> filesChunkOnNodesTable = new ConcurrentHashMap<String, Hashtable<String, HashSet<Integer>>>();
	private ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> processingFileDistributionTable = new ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>>();
	private static boolean isRunning;
	
	protected NameNode() throws RemoteException {
		super();
	}
	
	public static void main(String[] args) throws RemoteException {
		System.out.println("Starting name node server...");
		NameNode nameNode = new NameNode();
		isRunning = true;
		
		System.out.println("Loading configuration data...");
		try {
			IOUtil.readConf(PathConfiguration.DFSConfPath, nameNode);
			System.out.println("Configuration data loaded successfully.");
		} catch (IOException e1) {
			e1.printStackTrace();
			System.err.println("Failed loading configurations. System shutting down...");
			System.exit(-1);
		}
		
		
		NameNodeInterface stub = null;
		try {
			unexportObject(nameNode, false);
			stub = (NameNodeInterface) exportObject(nameNode,nameNodePort);
			nameNodeRegistry = LocateRegistry.createRegistry(nameNodeRegPort);
			nameNodeRegistry.rebind(nameNodeService, stub);
			System.out.println("Service \"" + nameNodeService + "\" has been set up on port: " + nameNodeRegPort + ".");
		} catch (RemoteException e) {
			e.printStackTrace();
			System.out.println("Server start failed. Shutting down...");
			System.exit(-1);
		} 
		
		nameNode.init();
		
		System.out.println("System is running...");
		while (isRunning) {
			
		}
		System.out.println("System is shuting down...");
	}

	/**
	 * Create a monitor thread to update data nodes status.
	 */
	public void init() {
		System.out.println("Initializing monitoring server...");
		NodeMonitor nodeMonitor = new NodeMonitor(this);
		Thread monitoring = new Thread(nodeMonitor);
		monitoring.start();
	}
	
	@Override
	public synchronized ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(
			ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> failureList) throws RemoteException {
		for (Entry<String, Hashtable<Integer, HashSet<String>>> fileTuple : failureList.entrySet()) {	
			//get filename
			String filename = fileTuple.getKey();
			for (Entry<Integer, HashSet<String>> chunkTuple : fileTuple.getValue().entrySet()) { 
				//get chunk number
				int chunkNum = chunkTuple.getKey();
				HashSet<String> newNodeList = new HashSet<String>();	//new dispatching node list
				for (String failedNode : chunkTuple.getValue()) {		//reclaim available slots on nodes
					this.dataNodeAvailableSlotList.put(failedNode, this.dataNodeAvailableSlotList.get(failedNode) + 1);
				}
				
				//re-dispatch chunks to nodes
				int nodeCount = chunkTuple.getValue().size();
				for (int i = 0; i < nodeCount; i++) {
					HashSet<String> excludeList = chunkTuple.getValue();
					String pickNode = null;
					pickNode = pickMostAvailableSlotDataNode(excludeList);
					newNodeList.add(pickNode);
					excludeList.add(pickNode);
				}
				
				//put back to failureList
				failureList.get(filename).put(chunkNum, newNodeList);
			}
		}
		return failureList;
	}
	
	@Override
	public synchronized ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(
			String filename, int chunkAmount) throws RemoteException {
		//check and update file status table to avoid duplicated file name
		if (this.fileStatusTable.containsKey(filename)) {		
			System.out.println("File name exist. Please try another.");
			throw new RemoteException();
		} else {
			fileStatusTable.put(filename, FileStatus.INPROGRESS);	
		}
		
		//chunkDispatchTable is used to store the dispatch result for this file
		Hashtable<Integer, HashSet<String>> chunkDispatchTable = new Hashtable<Integer, HashSet<String>>();
		for (int currentChunk = 0; currentChunk < chunkAmount; currentChunk++) {		//dispatch by chunks
			int replicaCount = this.replicaNum;
			HashSet<String> nodeList = new HashSet<String>();	//dispatched nodes list
			
			//dispatch by replica
			while (replicaCount > 0) {
				//pick the most available data node without those already in nodeList, 
				//exclude no nodes at first time
				String pickNode = null;
				try {
					pickNode = pickMostAvailableSlotDataNode(nodeList);
				} catch (RemoteException e) {
					throw e;	//if all nodes are full, dispatch failed
				}
				
				//check if chunkDispatchTable has current chunk, if not, create that hash set 
				if (!chunkDispatchTable.containsKey(currentChunk)) {	
					nodeList.add(pickNode);
					chunkDispatchTable.put(currentChunk, nodeList);
				} else {	//if chunk exists, add nodes into it
					chunkDispatchTable.get(currentChunk).add(pickNode);
				}
				replicaCount--;
			}
		}
		
		//store dispatch result to a temporary table, which will be updated to 
		//fileDistributionTable when dispatch succeed
		this.processingFileDistributionTable.put(filename, chunkDispatchTable);
		
		ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> resultTable = 
				new ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>>();
		resultTable.put(filename, chunkDispatchTable);
		System.out.println("Sending out chunk distribution list...");
		return resultTable;
	}
	
	/**
	 * Pick a data node that is most available for data storage.
	 * @param excludeList HashSet<String> A list of data nodes to be excluded from selection.
	 * @return String A data node selected.
	 * @throws RemoteException
	 */
	public synchronized String pickMostAvailableSlotDataNode(HashSet<String> excludeList) throws RemoteException {
		String minLoadDataNode = null;
		int mostAvailableSlots = Integer.MIN_VALUE;
		for (Entry<String, Integer> dataNodeTuple : this.dataNodeAvailableSlotList.entrySet()) {
			String dataNode = dataNodeTuple.getKey();
			if (this.dataNodeStatusList.get(dataNode) == NodeStatus.HEALTHY 
					&& !excludeList.contains(dataNode) 
					&& dataNodeTuple.getValue() > mostAvailableSlots) {
				minLoadDataNode = dataNodeTuple.getKey();
				mostAvailableSlots = dataNodeTuple.getValue();
				//preserve available slot for dispatching
				this.dataNodeAvailableSlotList.put(minLoadDataNode, mostAvailableSlots - 1);
			}
		}
		
		//if there is no space for dispatch
		if (minLoadDataNode == null) {
			throw new RemoteException("There is no data node available now.");
		}
		return minLoadDataNode;
	}
	
	@Override
	public boolean fileDistributionConfirm(String filename) {
		this.fileDistributionTable.put(filename, this.processingFileDistributionTable.get(filename));
		this.processingFileDistributionTable.remove(filename);
		this.fileStatusTable.put(filename, FileStatus.SUCCESS);
		System.out.println(filename + " has been uploaded to DFS.");
		return true;
	}
	

//	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable(
//			String filename) {
//		ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> returnList = new ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>>();
//		returnList.put(filename, this.fileDistributionTable.get(filename));
//		return returnList;
//	}

	@Override
	public void removeChunkFromFileDistributionTable(String filename, int chunkNum, String dataNodeIP) {
		if (fileDistributionTable != null 
				&& fileDistributionTable.containsKey(filename)
				&& fileDistributionTable.get(filename).containsKey(chunkNum)) {
			this.fileDistributionTable.get(filename).get(chunkNum).remove(dataNodeIP);
			System.out.println("Chunk" + chunkNum + " of file \"" +filename + "\" on " 
					+ dataNodeIP + " has been removed from file distribution table...");
		}
		return;
	}

	
//	public void updateFileDistributionTable(
//			ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> tableToBeUpdated) {
//		for (Entry<String, Hashtable<Integer, HashSet<String>>> fileTuple : tableToBeUpdated.entrySet()) {
//			String filename = fileTuple.getKey();
//			if (!this.fileDistributionTable.containsKey(filename)) {
//				fileDistributionTable.put(filename, fileTuple.getValue());
//				fileStatusTable.put(filename, FileStatus.INPROGRESS);
//			} else {
//				for (Entry<Integer, HashSet<String>> chunkTuple : fileTuple.getValue().entrySet()) {
//					int chunkNum = chunkTuple.getKey();
//					if (!fileDistributionTable.get(filename).containsKey(chunkNum)) {
//						fileDistributionTable.get(filename).put(chunkNum,chunkTuple.getValue());
//					} else {
//						fileDistributionTable.get(filename).get(chunkNum).addAll(chunkTuple.getValue());
//					}
//				}
//			}
//		}
//		System.out.println("File distribution table has been successfully updated...");
//		return;
//	}

	@Override
	public void registerDataNode(String dataNodeIP, int availableSlot) throws RemoteException{
		this.dataNodeAvailableSlotList.put(dataNodeIP, availableSlot);
		this.dataNodeStatusList.put(dataNodeIP, NodeStatus.HEALTHY);
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
	
	@Override
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

	@Override
	public boolean fileExist(String filename) {
		return this.fileDistributionTable.containsKey(filename);
	}

	@Override
	public ConcurrentHashMap<String, Integer> getDataNodeAvailableSlotList() {
		return this.dataNodeAvailableSlotList;
	}
	
	@Override
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable() {
		return this.fileDistributionTable;
	}
	
	@Override
	public ConcurrentHashMap<String, FileStatus> getFileStatusTable() {
		return this.fileStatusTable;
	}
	
	@Override
	public void terminate() {
		isRunning = false;
	}

	public ConcurrentHashMap<String, Hashtable<String, HashSet<Integer>>> getFilesChunkOnNodesTable() {
		return filesChunkOnNodesTable;
	}

	public void setFilesChunkOnNodesTable(
			ConcurrentHashMap<String, Hashtable<String, HashSet<Integer>>> filesChunkOnNodesTable) {
		this.filesChunkOnNodesTable = filesChunkOnNodesTable;
	}
}
