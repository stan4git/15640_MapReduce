/**
 * 
 */
package mapred;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;

import util.IOUtil;

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
	
	TaskTrackerInterface taskTracker;
	private static Integer taskTrackerRegPort;
	private static String taskTrackServiceName;
	
	public TaskThread () {
		
		try {
			Registry registry = LocateRegistry.getRegistry(curNode,taskTrackerRegPort);
			taskTracker = (TaskTrackerInterface) registry.lookup(taskTrackServiceName);
			
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		if(isMapTask) {
			
		} else {
			taskTracker.registerReduceTask(jobID, partitionNo, nodesWithPartitions);
		}
	}

}
