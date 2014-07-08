package mapred;

import java.rmi.Remote;
import java.util.ArrayList;
import java.util.HashMap;

public interface TaskTrackerInterface extends Remote {
	
	public byte[] getPartitionContent(String path);

	public void registerMapperTask(int jobID, JobConfiguration jobConf, HashMap<Integer, String> chunkSets);

	public void registerReduceTask(int jobID, int partitionNo, HashMap<String, ArrayList<String>> nodesWithPartitions, int numOfPartitions);
	
	public void remove(int jobID);
}
