/**
 * 
 */
package mapred;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;

import util.IOUtil;

/**
 * This class is a thread to connect to the specific task 
 * node and handling some failures.
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public class TaskThread implements Runnable {
	// current node
	private String curNode;
	private int jobID;
	private JobConfiguration jobConf;
	// chunk - > nodes
	private HashMap<Integer,String> chunkSets;
	private boolean isMapTask;
	// the partition number
	private int partitionNo;
	// a table which indicates the nodes and partitions
	private HashMap<String, ArrayList<String>> nodesWithPartitions;
	// number of partitions
	private int numOfPartitions;
	
	private TaskTrackerInterface taskTracker;
	// task tracker's registry port and service name
	private Integer taskTrackerRegPort;
	private String taskTrackServiceName;
	
	public TaskThread (String node, int jobID, JobConfiguration jobConf,
			HashMap<Integer,String> chunkSets, Boolean isMapTask, int partitionNo, 
			HashMap<String, ArrayList<String>> nodesWithPartitions, int numOfPartitions,
			Integer taskTrackerRegPort,String taskTrackServiceName) {
		this.curNode = node;
		this.jobID = jobID;
		this.jobConf = jobConf;
		this.chunkSets = chunkSets;
		this.isMapTask = isMapTask;
		this.partitionNo = partitionNo;
		this.nodesWithPartitions = nodesWithPartitions;
		this.numOfPartitions = numOfPartitions;
		this.taskTrackerRegPort = taskTrackerRegPort;
		this.taskTrackServiceName = taskTrackServiceName;
	}

	@Override
	public void run() {
		try {
			Registry registry = LocateRegistry.getRegistry(curNode,taskTrackerRegPort);
			taskTracker = (TaskTrackerInterface) registry.lookup(taskTrackServiceName);
		} catch (RemoteException | NotBoundException e) {
			if(isMapTask) {
				try {
					JobTracker.handleMapperFailure(jobID, curNode, chunkSets.keySet());
				} catch (RemoteException e1) {
					e1.printStackTrace();
				}
			} else {
				try {
					JobTracker.handleReducerFailure(jobID, partitionNo);
				} catch (RemoteException e1) {
					e1.printStackTrace();
				}
			}
			System.err.println("Cannot connect to the desired TaskTracker!!");
			System.exit(-1);
		}
		
		if(isMapTask) {
			try {
				taskTracker.registerMapperTask(jobID, jobConf, chunkSets);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			try {
				taskTracker.registerReduceTask(jobID, partitionNo, nodesWithPartitions, numOfPartitions);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
