package mapred;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import dfs.NameNodeInterface;

/**
 * This class is used to help the JobTracker to schedule the nodes for different
 * functions. It contains 4 basic methods: 1) init() It is used to create some
 * RMI connection; 2) selectBestNodeTochunk() It is used to select the best node
 * for fetching the specific chunk; 3) selectBestNodesForReduce() It is used to
 * select the best node for doing the reducer job; 4) chooseLightWorkLoadNode()
 * It is used to select the lightest workload node.
 * 
 * @author menglonghe
 * @author sidilin
 * 
 */
public class JobScheduler {

	private static NameNodeInterface nameNode = null;

	 //The maximum task numbers including mapper and reducer per node
	private static Integer maxTaskPerNode;
	// the weight of local nodes and global nodes
	private static Double localWeight;
	private static Double globalWeight;

	/* These 3 contains NameNode's registry IP,registry port and service name */
	private static String nameNodeIP;
	private static Integer nameNodeRegPort;
	private static String nameNodeService;

	// default constructor
	public JobScheduler(String nameNodeIPVal,Integer nameNodeRegPortVal,String nameNodeServiceVal,
			Integer maxTaskPerNodeVal,Double localWeightVal,Double globalWeightVal) {
		nameNodeIP = nameNodeIPVal;
		nameNodeRegPort = nameNodeRegPortVal;
		nameNodeService = nameNodeServiceVal;
		maxTaskPerNode = maxTaskPerNodeVal;
		localWeight = localWeightVal;
		globalWeight = globalWeightVal;
		try {
			init();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This is a method to initialize the class. 
	 * It gets the ROR from the namenode.
	 * @throws IOException 
	 */
	public void init() throws IOException {
		Registry reigstry;
		try {
			reigstry = LocateRegistry.getRegistry(nameNodeIP, nameNodeRegPort);
			nameNode = (NameNodeInterface) reigstry.lookup(nameNodeService);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method is used to select the best node to execute the specific chunk
	 * of the input file.
	 * 
	 * The rule is calculating the two selected nodes: local best node and
	 * global best node. We then using the programmer's setting of the local
	 * weight and global weight to decide the final node.
	 * 
	 * @param chunkDistribution
	 *            the chunks of the input file and their distribution list
	 * @return HashMap<String, HashMap<Integer, String>> which node to execute
	 *         the chunk and where to fetch the chunk
	 * @throws RemoteException 
	 */
	public HashMap<String, HashMap<Integer, String>> selectBestNodeToChunks(
			Hashtable<Integer, HashSet<String>> chunkDistribution) throws RemoteException {
		HashMap<String, HashMap<Integer, String>> res = new HashMap<String, HashMap<Integer, String>>();
		for (int chunkNum : chunkDistribution.keySet()) {
			HashSet<String> healthyNodes = nameNode.getHealthyNodes();

			// step1 : select the best nodes of the replication
			HashSet<String> nodes = chunkDistribution.get(chunkNum);
			for (String node : nodes) {
				if (!healthyNodes.contains(node)) {
					healthyNodes.remove(node);
				}
			}
			if (nodes.size() == 0) {
				System.out.println("It don't allow all the replications down!");
				return null;
			}
			String bestLocalNode = chooseLightWorkloadNode(nodes);

			// step2 : select the best nodes from all the other nodes except the
			// local nodes

			for (String node : nodes) {
				if (healthyNodes.contains(node)) {
					healthyNodes.remove(node);
				}
			}
			String bestGlobalNode = chooseLightWorkloadNode(healthyNodes);

			// step3: compare the two different nodes

			if (bestLocalNode == null && bestGlobalNode == null) {
				return null;
			}

			String finalNode = null;
			if (bestLocalNode != null && bestGlobalNode != null) {
				double localNodePoint = (maxTaskPerNode - JobTracker.node_totalTasks
						.get(bestLocalNode)) * localWeight;
				double globalNodePoint = (maxTaskPerNode - JobTracker.node_totalTasks
						.get(bestGlobalNode)) * globalWeight;
				finalNode = localNodePoint >= globalNodePoint ? bestLocalNode
						: bestGlobalNode;
			}

			if (bestLocalNode == null || bestGlobalNode == null) {
				finalNode = bestLocalNode == null ? bestGlobalNode
						: bestLocalNode;
			}

			// step4 : organize the return info
			// JobTracker.node_totalTasks.put(finalNode,
			// JobTracker.jobID_totalMapTasks.get(finalNode) + 1);
			HashMap<Integer, String> chunkAndSourceNode = null;
			if (!res.containsKey(finalNode)) {
				chunkAndSourceNode = new HashMap<Integer, String>();
			} else {
				chunkAndSourceNode = res.get(finalNode);
			}
			chunkAndSourceNode.put(chunkNum, bestLocalNode);
			res.put(finalNode, chunkAndSourceNode);
		}
		return res;
	}

	/**
	 * This class is used to select the best node which has the lightest
	 * workload in a range of nodes.
	 * 
	 * @param nodes
	 *            the selected nodes including the local nodes and healthy
	 *            global nodes.
	 * 
	 * @return String the chosen node which represents by its IP address
	 */
	public String chooseLightWorkloadNode(HashSet<String> nodes) {
		if (nodes == null) {
			return null;
		}
		String lightestNode = null;
		int minWorkLoad = maxTaskPerNode + 1;
		for (String node : nodes) {
			int num = JobTracker.node_totalTasks.get(node);
			if (lightestNode == null
					|| num < JobTracker.node_totalTasks.get(lightestNode)) {
				lightestNode = node;
				minWorkLoad = num;
			}
		}
		if (minWorkLoad == maxTaskPerNode + 1) {
			return null;
		}

		return lightestNode;
	}

	/**
	 * This classed is used to select the best nodes to execute the Reducer
	 * work. It just chooses the node with the most light workload and they are
	 * healthy.
	 * 
	 * @param numOfReducers
	 *            the number of partitions or reducers.
	 * @return ArrayList<String> the select nodes
	 * @throws RemoteException 
	 */
	public ArrayList<String> pickBestNodesForReduce(int numOfReducers) throws RemoteException {

		ArrayList<String> nodesForReduce = new ArrayList<String>();
		HashSet<String> availableNodes = nameNode.getHealthyNodes();
		int numOfChosen = 0;

		while (numOfChosen < numOfReducers) {
			int minWorkLoad = maxTaskPerNode + 1;
			String chosenReducer = null;
			for (String node : availableNodes) {
				int workLoad = JobTracker.node_totalTasks.get(node);
				if (workLoad < minWorkLoad) {
					minWorkLoad = workLoad;
					chosenReducer = node;
				}
			}
			if (minWorkLoad == maxTaskPerNode + 1) {
				return null;
			}
			nodesForReduce.add(chosenReducer);
		}

		return nodesForReduce;
	}

}
