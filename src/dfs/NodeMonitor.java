package dfs;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import util.*;

/**
 * 1. check heart beat of dataNode
 * 2. check replica amount
 * 3. check write - confirm copy is made
 * 4. assign to node with most available - mention in report
 * 5. copy from next datanode
 * 6. check point
 */
public class NodeMonitor implements Runnable {
	private NameNode nameNodeInstance;
	private boolean isRunning;
	private ConcurrentHashMap<String, DataNodeInterface> dataNodeServiceList;
	private int dataNodeRegPort;
	private String dataNodeService;
	private int heartbeatCheckThreshold;
	private int heartbeatInterval;
	
	
	public NodeMonitor(NameNode nameNodeInstance) {
		this.nameNodeInstance = nameNodeInstance;
		this.dataNodeServiceList = new ConcurrentHashMap<String, DataNodeInterface>();
	}
	
	
	public void run() {
		this.isRunning = true;
		try {
			IOUtil.readConf(PathConfiguration.DFSConfPath, this);
			System.out.println("Monitoring...");
		} catch (IOException e1) {
			e1.printStackTrace();
			System.err.println("Loading configuration failed.");
			System.exit(-1);
		}
		
		while (isRunning) {
			updateNodeStatus();
			updateAvailableSlot();
			try {
				Thread.sleep(this.heartbeatInterval * 1000);
			} catch (InterruptedException e) {
				continue;
			}
		}
	}
	
	
	public void updateNodeStatus() {
		for (Entry<String, NodeStatus> nodeStatus : nameNodeInstance.getDataNodeStatusList().entrySet()) {
			String dataNodeIP = nodeStatus.getKey();
			DataNodeInterface dataNodeService = null;
			int retryThreshold = this.heartbeatCheckThreshold;
			while (retryThreshold > 0) {
			try {
				dataNodeService = getDataNodeService(dataNodeIP);
				dataNodeService.heartbeat();
				break;
//				this.nameNodeInstance.getDataNodeStatusList().put(dataNodeIP, NodeStatus.HEALTHY);
			} catch (Exception e2) {
				retryThreshold--;
				if (retryThreshold <= 0) {
					System.err.println(dataNodeIP + " is down. Recovering data...");
					try {
						if(this.nameNodeInstance.getFilesChunkOnNodesTable().containsKey(dataNodeIP)){
							ensureReplica(dataNodeIP, this.nameNodeInstance.getFilesChunkOnNodesTable().get(dataNodeIP));
						}
					} catch (Exception e1) {
						e1.printStackTrace();
						System.err.println("Cannot recover data from " + dataNodeIP + "'s failure...");
						return;
					}
					
					//clean up the dead node information
					System.out.println("Cleaning up " + dataNodeIP + "'s information...");
					this.nameNodeInstance.getDataNodeStatusList().remove(dataNodeIP);
					this.nameNodeInstance.getDataNodeAvailableSlotList().remove(dataNodeIP);
					this.nameNodeInstance.getFilesChunkOnNodesTable().remove(dataNodeIP);
					System.out.println(dataNodeIP + " has been removed from name node.");
					return;
				}
			}
			}
		}
	}
	
	
	public void updateAvailableSlot() {
		for (Entry<String, Integer> nodeTuple : nameNodeInstance.getDataNodeAvailableSlotList().entrySet()) {
			String dataNodeIP = nodeTuple.getKey();
			DataNodeInterface dataNodeService = null;
			try {
				//System.out.println("Connecting " + dataNodeIP + "...");
				dataNodeService = getDataNodeService(dataNodeIP);
				//System.out.println("Connected to " + dataNodeIP + ".");
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Cannot connect to " + dataNodeIP + ".");
				return;
			}
			
			int nodeValue;
			try {
				//System.out.println("Updating " + dataNodeIP + "'s available list...");
				nodeValue = dataNodeService.getAvailableChunkSlot();
				//System.out.println(dataNodeIP + "has " + nodeValue + " available slots...");
			} catch (RemoteException e) {
				e.printStackTrace();
				continue;
			}
			int nameNodeValue = nodeTuple.getValue();
			int newValue = (nodeValue < nameNodeValue) ? nodeValue : nameNodeValue;
			//System.out.println("Updating available slots table...");
			nameNodeInstance.getDataNodeAvailableSlotList().put(dataNodeIP, newValue);
		}
		return;
	}
	
	
	public void ensureReplica(String deadNode, Hashtable<String, HashSet<Integer>> missingChunkList) throws Exception {
		for (Entry<String, HashSet<Integer>> fileTuple : missingChunkList.entrySet()) {
			String filename = fileTuple.getKey();
			
			//copy chunks from other nodes
			for (int chunkNum : fileTuple.getValue()) {
				HashSet<String> excludeList = this.nameNodeInstance.getFileDistributionTable().get(filename).get(chunkNum);
				String moveToNode = null;
				
				try {	//find a node to move to 
					moveToNode = this.nameNodeInstance.pickMostAvailableSlotDataNode(excludeList);
				} catch (RemoteException e1) {
					e1.printStackTrace();
					System.out.println("Cannot find any available node...");
					throw (new Exception());
				}
				
				//find a node to move from
				String moveFromNode = null;
				for (String node : excludeList) {
					if (!node.equals(deadNode)) {
						moveFromNode = node;
						System.out.println("Message sent to " + moveToNode + " to download "
								+ filename + "_" + chunkNum + " from " + moveFromNode);
						try {	//download chunks from another node
							this.dataNodeServiceList.get(moveToNode).downloadChunk(filename, chunkNum, moveFromNode);
							
							//update file distribution list <filename, <chunkNum, nodeList>>
							this.nameNodeInstance.getFileDistributionTable().get(filename).get(chunkNum).remove(moveFromNode);
							this.nameNodeInstance.getFileDistributionTable().get(filename).get(chunkNum).add(moveToNode);
							
							//update chunks on nodes table <node,<filename, chunkSet>>
							HashSet<Integer> chunkList = new HashSet<Integer>();
							chunkList.addAll(this.nameNodeInstance.getFilesChunkOnNodesTable().get(moveToNode).get(filename));
							chunkList.add(chunkNum);
							this.nameNodeInstance.getFilesChunkOnNodesTable().get(moveToNode).put(filename, chunkList);

							//done with the chunk moving
							break;
						} catch (RemoteException e) {
							e.printStackTrace();
							continue;
						}
					}
				}
				
				//if no node available for this chunk, 
				//that means we lost this chunk, file broken, set it to failed
				if (moveFromNode == null) {
					this.nameNodeInstance.getFileStatusTable().put(filename, FileStatus.FAILED);
					throw (new RemoteException());
				}
				System.out.println("Finished copying " + filename + "_" + chunkNum 
						+ " from " + moveFromNode + " to " + moveToNode + "...");
			}
			System.out.println(filename + " is recovered...");
		}
		return;
	}
	
	
	
	private DataNodeInterface getDataNodeService(String dataNodeIP) throws Exception {
		if (!this.dataNodeServiceList.contains(dataNodeIP)) {
			try {
				Registry dataNodeRegistry = LocateRegistry.getRegistry(dataNodeIP, this.dataNodeRegPort);
				DataNodeInterface dataNode = (DataNodeInterface) dataNodeRegistry.lookup(this.dataNodeService);
				this.dataNodeServiceList.put(dataNodeIP, dataNode);
			} catch (RemoteException | NotBoundException e) {
				System.err.println("Cannot connect to " + dataNodeIP + "...");
				throw e;
			}
		}
		return this.dataNodeServiceList.get(dataNodeIP);
	}
	
	
	@SuppressWarnings("unused")
	private void terminate() {
		this.isRunning = false;
	}
}
