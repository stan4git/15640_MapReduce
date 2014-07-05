package mapred;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

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
	private static Integer mapperChunkThreshold;
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
