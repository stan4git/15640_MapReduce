package dfs;
/**
 * 1. check heart beat of dataNode
 * 2. check replica amount
 * 3. check write - confirm copy is made
 * 4. assign to node with most available - mention in report
 * 5. copy from next datanode
 * 6. check point
 */
public class NodeMonitor implements Runnable {
	private NameNode mainClass;
	
	public NodeMonitor(NameNode mainClassInstance) {
		this.mainClass = mainClassInstance;
	}
	
	
	public void checkNodeStatus() {
		
	}
	
	public void ensureReplica() {
		
	}

	private void removeNode() {
		
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
}
