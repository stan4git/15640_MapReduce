package mapred;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

import util.IOUtil;
import dfs.NameNodeInterface;

/**
 * 1. decide mapper (return a list)
 * 2. pickBestNode() - local/global weight * available slots
 * 3. pickBestNodesForReduce() - weight(Mapper + Reduce tasks)
 */
public class JobScheduler {
	
	private static NameNodeInterface nameNode = null;
	
	private static Integer maxTaskPerNode;
	private static Double localWeight;
	private static Double globalWeight;
	
	/*These 3 contains NameNode's registry IP,registry port and service name*/
	private static String nameNodeIP;
	private static Integer nameNodeRegPort;
	private static String nameNodeService;
	
	private static String DFSConfPath = "conf/dfs.conf";
	private static String MapReduceConfPath = "conf/mapred.conf";
	
	public JobScheduler(){
		init();
	}
	
	public void init(){
		JobScheduler jobScheduler = new JobScheduler();
		IOUtil.readConf(DFSConfPath, jobScheduler);
		IOUtil.readConf(MapReduceConfPath, jobScheduler);
		Registry reigstry;
		try {
			reigstry = LocateRegistry.getRegistry(nameNodeIP, nameNodeRegPort);
			nameNode = (NameNodeInterface)reigstry.lookup(nameNodeService);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	
	public HashMap<String, HashMap<Integer, String>> selectBestNodeToChunks(
			Hashtable<Integer, HashSet<String>> chunkDistribution) {
		HashMap<String, HashMap<Integer, String>> res = new HashMap<String, HashMap<Integer, String>>();
		for(int chunkNum : chunkDistribution.keySet()) {
			HashSet<String> healthyNodes = nameNode.getHealthyNodes();
			
			// step1 : select the best nodes of the replication
			HashSet<String> nodes = chunkDistribution.get(chunkNum);
			for(String node : nodes) {
				if(!healthyNodes.contains(node)) {
					healthyNodes.remove(node);
				}
			}
			if(nodes.size() == 0) {
				System.out.println("It don't allow all the replications down!");
				return null;
			}
			String bestLocalNode = chooseLightWorkloadNode(nodes);
			
			// step2 : select the best nodes from all the other nodes except the local nodes
			
			for(String node : nodes) {
				if(healthyNodes.contains(node)) {
					healthyNodes.remove(node);
				}
			}
			String bestGlobalNode = chooseLightWorkloadNode(healthyNodes);
			
			// step3: compare the two different nodes
			
			double localNodePoint = (maxTaskPerNode - JobTracker.node_totalTasks.get(bestLocalNode)) * localWeight;
			double globalNodePoint = (maxTaskPerNode - JobTracker.node_totalTasks.get(bestGlobalNode)) * globalWeight;
			String finalNode = localNodePoint >= globalNodePoint ? bestLocalNode : bestGlobalNode;
			
			// step4 : organize the return info
			//JobTracker.node_totalTasks.put(finalNode, JobTracker.jobID_totalMapTasks.get(finalNode) + 1);
			
			HashMap<Integer,String> chunkAndSourceNode = new HashMap<Integer,String>();
			chunkAndSourceNode.put(chunkNum, bestLocalNode);
			res.put(finalNode, chunkAndSourceNode);
		}
		return res;
	}
	
	public String chooseLightWorkloadNode(HashSet<String> nodes) {
		if(nodes == null) {
			return null;
		}
		String lightestNode = null;
		for(String node : nodes) {
			int num = JobTracker.node_totalTasks.get(node);
			if(lightestNode == null || num < JobTracker.node_totalTasks.get(lightestNode)) {
				lightestNode = node;
			}
		}
		return lightestNode;
	}
	
	public ArrayList<String> pickBestNodesForReduce(int numOfReducers) {
		
		ArrayList<String> nodesForReduce = new ArrayList<String>();
		HashSet<String> availableNodes = nameNode.getHealthyNodes();
		int numOfChosen = 0;
		
		while(numOfChosen < numOfReducers) {
			int minWorkLoad = maxTaskPerNode + 1;
			String chosenReducer = null;
			for(String node : availableNodes) {
				int workLoad = JobTracker.node_totalTasks.get(node);
				if( workLoad < minWorkLoad) {
					minWorkLoad = workLoad;
					chosenReducer = node;
				}
			}
			if(minWorkLoad == maxTaskPerNode + 1) {
				return null;
			}
			nodesForReduce.add(chosenReducer);
		}
		
		return nodesForReduce;
	}

}
