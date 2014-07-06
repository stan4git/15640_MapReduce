package dfs;

import java.util.Map.Entry;

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
	
	public NodeMonitor(NameNode nameNodeInstance) {
		this.nameNodeInstance = nameNodeInstance;
	}
	
	
	public void checkNodeStatus() {
		for (Entry<String, NodeStatus> nodeStatus : nameNodeInstance.getDataNodeStatusList().entrySet()) {
			
		}
	}
	
	public void ensureReplica() {
		
	}

	private void removeDeadNode() {
		
	}
	
	
	private void terminate() {
		this.isRunning = false;
	}
	@Override
	public void run() {
		while (isRunning) {
			checkNodeStatus();
			ensureReplica();
			removeDeadNode();
		}
	}
}
