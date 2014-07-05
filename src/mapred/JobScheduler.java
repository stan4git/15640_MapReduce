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
	
	private static final String dfsConf = "conf/dfs.conf";
	private static final String mapredConf = "conf/mapred.conf";
	
	private static NameNodeInterface nameNode = null;
	private static JobTrackerInterface jobTracker = null;
	
	private static String nameNodeIP;
	private static Integer nameNodeRegPort;
	private static String nameNodeService;
	private static String jobTrackerIP;
	private static Integer jobTrackerRegPort;
	private static String jobTrackServiceName;
	private static Integer maxTask;
	
	public JobScheduler() {
		
		IOUtil.readConf(dfsConf, this);
		IOUtil.readConf(mapredConf, this);
		
		try {
			Registry reigstry = LocateRegistry.getRegistry(nameNodeIP, nameNodeRegPort);
			nameNode = (NameNodeInterface)reigstry.lookup(nameNodeService);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	
	public static ArrayList<String> pickBestNodesForReduce(int numOfReducers) {
		
		ArrayList<String> nodesForReduce = new ArrayList<String>();
		HashSet<String> availableNodes = nameNode.getAvailableNodes();
		int numOfChosen = 0;
		
		while(numOfChosen < numOfReducers) {
			int minWorkLoad = maxTask + 1;
			String chosenReducer = null;
			for(String node : availableNodes) {
				int workLoad = JobTracker.node_totalTasks.get(node);
				if( workLoad < minWorkLoad) {
					minWorkLoad = workLoad;
					chosenReducer = node;
				}
			}
			if(minWorkLoad == maxTask + 1) {
				return null;
			}
			nodesForReduce.add(chosenReducer);
		}
		
		return nodesForReduce;
	}

}
