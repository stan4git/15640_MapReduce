package mapred;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;

public interface TaskTrackerInterface extends Remote {
	
	public byte[] getPartitionContent(String path) throws RemoteException, IOException;

	public void registerMapperTask(int jobID, JobConfiguration jobConf, HashMap<Integer, String> chunkSets) throws RemoteException;

	public void registerReduceTask(int jobID, int partitionNo, HashMap<String, ArrayList<String>> nodesWithPartitions, int numOfPartitions) throws RemoteException;
	
	public void remove(int jobID);
}
