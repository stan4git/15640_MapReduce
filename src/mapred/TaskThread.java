/**
 * 
 */
package mapred;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @author menglonghe
 * @author sidilin
 *
 */
public class TaskThread implements Runnable {
	
	private boolean isMapTask;
	private int jobID;
	private int partitionNo;
	private HashSet<String> nodesWithPartitions;
	private String curNode;
	private JobConfiguration jobConf;
	private HashMap<Integer,String> chunkSets;
	private TaskTrackerInterface taskTracker;
	private static Integer taskTrackerRegPort;
	private static String taskTrackServiceName;
	
	public TaskThread (String node, int jobID, JobConfiguration jobConf,HashMap<Integer,String> chunkSets, Boolean isMapTask) {
		this.curNode = node;
		this.jobID = jobID;
		this.jobConf = jobConf;
		this.chunkSets = chunkSets;
		this.isMapTask = isMapTask;
	}

	@Override
	public void run() {
		try {
			Registry registry = LocateRegistry.getRegistry(curNode,taskTrackerRegPort);
			taskTracker = (TaskTrackerInterface) registry.lookup(taskTrackServiceName);
			
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		
		if(isMapTask) {
			taskTracker.registerMapperTask(jobID,jobConf,chunkSets);
		} else {
			taskTracker.registerReduceTask(jobID, partitionNo, nodesWithPartitions);
		}
	}

}
