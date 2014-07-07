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
	private ConcurrentHashMap<String, DataNodeInterface> dataNodeList;
	private int dataNodePort;
	private String dataNodeService;
	private int heartbeatCheckThreshold;
	private int heartbeatInterval;
	
	
	public NodeMonitor(NameNode nameNodeInstance) {
		this.nameNodeInstance = nameNodeInstance;
		this.dataNodeList = new ConcurrentHashMap<String, DataNodeInterface>();
	}
	
	
	public void run() {
		IOUtil.readConf("conf/dfs.conf", this);
		while (isRunning) {
			updateNodeStatus();
			try {
				Thread.sleep(this.heartbeatInterval * 1000);
			} catch (InterruptedException e) {
				continue;
			}
		}
	}
	
	
	public void updateNodeStatus() {
		ConcurrentHashMap<String, NodeStatus> returnList = new ConcurrentHashMap<String, NodeStatus>();
		
		for (Entry<String, NodeStatus> nodeStatus : nameNodeInstance.getDataNodeStatusList().entrySet()) {
			String dataNodeIP = nodeStatus.getKey();
			if (!this.dataNodeList.contains(dataNodeIP)) {
				try {
					Registry dataNodeRegistry = LocateRegistry.getRegistry(dataNodeIP, this.dataNodePort);
					DataNodeInterface dataNode = (DataNodeInterface) dataNodeRegistry.lookup(this.dataNodeService);
					this.dataNodeList.put(dataNodeIP, dataNode);
				} catch (RemoteException e) {
					e.printStackTrace();
				} catch (NotBoundException e) {
					e.printStackTrace();
				}
			}
			
			int retryThreshold = this.heartbeatCheckThreshold;
			while (retryThreshold > 0) {
				retryThreshold--;
				try {
					this.dataNodeList.get(dataNodeIP).heartbeat();
					returnList.put(dataNodeIP, NodeStatus.HEALTHY);
					break;
				} catch (RemoteException e) {
					if (retryThreshold <= 0) {
						returnList.remove(dataNodeIP);
					}
				}
			}
		}
		
		//update data node status list
		nameNodeInstance.getDataNodeStatusList().putAll(returnList);
		return;
	}
	
	public void ensureReplica() {
		for (Entry<String, Hashtable<Integer, HashSet<String>>> fileDistribution : nameNodeInstance.getFileDistributionTable().entrySet()) {
			String filename = fileDistribution.getKey();
			for (Entry<Integer, HashSet<String>> chunkTuple : fileDistribution.getValue().entrySet()) {
				int chunkNum = chunkTuple.getKey();
				String[] nodeList = (String[]) chunkTuple.getValue().toArray();
				for (String node : chunkTuple.getValue()) {
					try {
						if (!this.dataNodeList.get(node).hasChunk(filename, chunkNum)) {
							String downloadFromNode = node;
							int pickNodeIndex = -1;
							while (downloadFromNode.equals(node)) {
								pickNodeIndex = (int) (Math.random() * (chunkTuple.getValue().size() - 1));
								downloadFromNode = nodeList[pickNodeIndex];
							}
							this.dataNodeList.get(node).downloadChunk(filename, chunkNum, nodeList[pickNodeIndex]);
							System.out.println("Message sent to " + node + " to download " 
									+ filename + "_" + chunkNum + " from " + nodeList[pickNodeIndex]);
						}
					} catch (RemoteException e) {
						e.printStackTrace();
						continue;
					}
				}
			}
		}
	}
	
	@SuppressWarnings("unused")
	private void terminate() {
		this.isRunning = false;
	}
}
