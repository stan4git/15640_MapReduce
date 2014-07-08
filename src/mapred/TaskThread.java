/**
 * 
 */
package mapred;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author menglonghe
 * @author sidilin
 *
 */
public class TaskThread implements Runnable {
	
	private String curNode;
	private int jobID;
	private JobConfiguration jobConf;
	private HashMap<Integer,String> chunkSets;
	private boolean isMapTask;
	private int partitionNo;
	private HashMap<String, ArrayList<String>> nodesWithPartitions;
	private int numOfPartitions;
	
	private TaskTrackerInterface taskTracker;
	private static Integer taskTrackerRegPort;
	private static String taskTrackServiceName;
	
	public TaskThread (String node, int jobID, JobConfiguration jobConf,
			HashMap<Integer,String> chunkSets, Boolean isMapTask, int partitionNo, 
			HashMap<String, ArrayList<String>> nodesWithPartitions, int numOfPartitions) {
		this.curNode = node;
		this.jobID = jobID;
		this.jobConf = jobConf;
		this.chunkSets = chunkSets;
		this.isMapTask = isMapTask;
		this.partitionNo = partitionNo;
		this.nodesWithPartitions = nodesWithPartitions;
		this.numOfPartitions = numOfPartitions;
	}

	@Override
	public void run() {
		try {
			Registry registry = LocateRegistry.getRegistry(curNode,taskTrackerRegPort);
			taskTracker = (TaskTrackerInterface) registry.lookup(taskTrackServiceName);
		} catch (RemoteException | NotBoundException e) {
			if(isMapTask) {
				JobTracker.handleMapperFailure(jobID);
			} else {
				JobTracker.handleReducerFailure(jobID);
			}
			System.err.println("Cannot connect to the desired TaskTracker!!");
		}
		
		if(isMapTask) {
			taskTracker.registerMapperTask(jobID, jobConf, chunkSets);
		} else {
			taskTracker.registerReduceTask(jobID, partitionNo, nodesWithPartitions, numOfPartitions);
		}
	}

}
