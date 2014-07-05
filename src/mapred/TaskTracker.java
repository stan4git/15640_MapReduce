package mapred;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import dfs.NameNodeInterface;
import format.KVPair;
import util.IOUtil;

/**
 * 1. Available task slots
 * 2. run mapRunner
 * 3. 
 */
public class TaskTracker extends UnicastRemoteObject implements TaskTrackerInterface {
	
	private static final long serialVersionUID = -897603125687983899L;

	private static JobTrackerInterface jobTracker = null;
	
	private static final String mapredConf = "conf/mapred.conf";
	
	private static String jobTrackerIP;
	private static Integer jobTrackerRegPort;
	private static String jobTrackServiceName;
	private static Integer taskPort;
	private static Integer taskTrackerRegPort;
	private static String taskTrackServiceName;
	
	public static ConcurrentHashMap<Integer, HashSet<String>> jobID_parFilePath = new ConcurrentHashMap<Integer, HashSet<String>>();
	
	
	private static String reducerClassName ;
	
	protected TaskTracker() throws RemoteException {
		super();
		
		try {
			Registry registry = LocateRegistry.getRegistry(jobTrackerIP, jobTrackerRegPort);
			jobTracker = (JobTrackerInterface)registry.lookup(jobTrackServiceName);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	public void registerReduceTask(int jobID, int partitionNo, HashSet<String> nodesWithPartitions) {
		
		localizeTask(jobID);
		startReduceTask(jobID, partitionNo, nodesWithPartitions, reducerClassName);
	}
	
	public void localizeTask (int jobID) {
		KVPair reducerInfo = jobTracker.getReducerInfo(jobID);
		reducerClassName = reducerInfo.getKey().toString().replace('.', '/') + ".class";
		byte[] reducerClassContent = (byte[])reducerInfo.getValue();
		IOUtil.writeBinary(reducerClassContent, reducerClassName);
	}
	
 	public void startReduceTask (int jobID, int partitionNo, HashSet<String> nodesWithPartitions, String className) {
 		ReduceRunner reduceRunner = new ReduceRunner(jobID, partitionNo, nodesWithPartitions, className);
		reduceRunner.start();
	}
 	
 	public String getPartitionContent (int jobID, int partitionNo) {
 		HashSet<String> pathsForJob = jobID_parFilePath.get(jobID);
		HashSet<String> pathsForPartition = new HashSet<String>();
		for(String path : pathsForJob) {
			pathsForPartition.add(path + partitionNo);
		}
 		return Merger.merge(pathsForPartition);
 	}
	
	public static void main (String args[]) {
		
		try {
			TaskTracker taskTracker = new TaskTracker();
			IOUtil.readConf(mapredConf, taskTracker);
			
			TaskTrackerInterface stub = (TaskTrackerInterface) exportObject (taskTracker, taskPort);
			Registry registry = LocateRegistry.createRegistry(taskTrackerRegPort);
			registry.rebind(taskTrackServiceName, stub);
			
			System.out.println("I'm the TaskTracker for node " + InetAddress.getLocalHost().getHostAddress());
			
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
