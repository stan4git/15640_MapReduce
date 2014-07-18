package mapred;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
/**
 * This is a interface class of Task Tracker
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public interface TaskTrackerInterface extends Remote {
	/**
	 * This method is used to get the partition contents
	 * @param path the partition file's path
	 * 
	 * @return byte array
	 * @throws RemoteException
	 * @throws IOException
	 */
	public byte[] getPartitionContent(String path) throws RemoteException, IOException;
	/**
	 * This method is used to register Mapper task including 
	 * split the chunks to several map workers and split them into 
	 * different nodes and initiate them.
	 * 
	 * @param jobID
	 * @param jobConf
	 * @param chunkSets
	 * @throws RemoteException
	 */
	public void registerMapperTask(int jobID, JobConfiguration jobConf, HashMap<Integer, String> chunkSets) throws RemoteException;
	/**
	 * This method is used to register Reducer task including
	 * select the best nodes to run the reduce task and start
	 * some threads to run the reduce work
	 * 
	 * @param jobID
	 * @param partitionNo
	 * @param nodesWithPartitions
	 * @param numOfPartitions
	 * @throws RemoteException
	 */
	public void registerReduceTask(int jobID, int partitionNo, HashMap<String, ArrayList<String>> nodesWithPartitions, int numOfPartitions) throws RemoteException;
	/**
	 * This method is used to remove the specific job according 
	 * to the job ID. It removes all the status and records in the 
	 * system.
	 * 
	 * @param jobID
	 */
	public void remove(int jobID) throws RemoteException;
	
	/***
	 * This method is used to respond to JobTracker every 5 seconds
	 * @return task status for each job 
	 */
	public ConcurrentHashMap<Integer, TaskStatusInfo>  heartBeat() throws RemoteException;
}
