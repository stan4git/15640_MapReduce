package dfs;

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
	private int dataNodePort;
	private int heartbeatCheckThreshold;
	private int heartbeatInterval;
	
	
	public NodeMonitor(NameNode nameNodeInstance) {
		this.nameNodeInstance = nameNodeInstance;
		this.dataNodeServiceList = new ConcurrentHashMap<String, DataNodeInterface>();
	}
	
	
	public void run() {
		IOUtil.readConf("conf/dfs.conf", this);
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
			dataNodeService = getDataNodeService(dataNodeIP);
			
			ConcurrentHashMap<String, NodeStatus> returnList = new ConcurrentHashMap<String, NodeStatus>();
			int retryThreshold = this.heartbeatCheckThreshold;
			while (retryThreshold > 0) {
				retryThreshold--;
				try {
					dataNodeService.heartbeat();
					returnList.put(dataNodeIP, NodeStatus.HEALTHY);
					break;
				} catch (RemoteException e) {
					if (retryThreshold <= 0) {
						//make file chunk duplicate
						//generate file dist list and get data node to download chunks
						//update file dis list
						
						
						
						returnList.remove(dataNodeIP);
						return;
					}
				}
			}
			
			
		}
		
		//update data node status list
		nameNodeInstance.getDataNodeStatusList().putAll(returnList);
		return;
	}
	
	
	public void updateAvailableSlot() {
		for (Entry<String, Integer> nodeTuple : nameNodeInstance.getDataNodeAvailableSlotList().entrySet()) {
			String dataNodeIP = nodeTuple.getKey();
			DataNodeInterface dataNodeService = null;
			try {
				dataNodeService = getDataNodeService(dataNodeIP);
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
			
			int nodeValue;
			try {
				nodeValue = dataNodeService.getAvailableChunkSlot();
			} catch (RemoteException e) {
				e.printStackTrace();
				continue;
			}
			int nameNodeValue = nodeTuple.getValue();
			int newValue = (nodeValue < nameNodeValue) ? nodeValue : nameNodeValue;
			nameNodeInstance.getDataNodeAvailableSlotList().put(dataNodeIP, newValue);
		}
		return;
	}
	
	
	public void ensureReplica(String deadNode, ConcurrentHashMap<String, HashSet<Integer>> missingChunkList) {
		for (Entry<String, HashSet<Integer>> fileTuple : missingChunkList.entrySet()) {
			String filename = fileTuple.getKey();
			for (int chunkNum : fileTuple.getValue()) {
				HashSet<String> excludeList = this.nameNodeInstance.getFileDistributionTable().get(filename).get(chunkNum);
				String moveToNode = this.nameNodeInstance.pickMostAvailableSlotDataNode(excludeList);
				
				for (String node : excludeList) {
					if (!node.equals(deadNode)) {
						
					}
				}
				
				
				
				
				System.out.println("Message sent to " + node + " to download "
						+ filename + "_" + chunkNum + " from "
						+ nodeList[pickNodeIndex]);
			}
		}
	}
	
	
	
	private DataNodeInterface getDataNodeService(String dataNodeIP) throws Exception {
		if (!this.dataNodeServiceList.contains(dataNodeIP)) {
			try {
				Registry dataNodeRegistry = LocateRegistry.getRegistry(dataNodeIP, this.dataNodePort);
				DataNodeInterface dataNode = (DataNodeInterface) dataNodeRegistry.lookup(this.dataNodeService);
				this.dataNodeServiceList.put(dataNodeIP, dataNode);
			} catch (RemoteException | NotBoundException e) {
				throw e;
			}
		}
		return this.dataNodeServiceList.get(dataNodeIP);
	}
	
	
	private void terminate() {
		this.isRunning = false;
	}
}
