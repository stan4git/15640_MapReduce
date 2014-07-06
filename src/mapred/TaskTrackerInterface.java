package mapred;

import java.rmi.Remote;
import java.util.HashMap;
import java.util.HashSet;

public interface TaskTrackerInterface extends Remote {
	
	public String getPartitionContent (int jobID, int partitionNo);

	public void registerReduceTask(int jobID, int partitionNo,
			HashSet<String> nodesWithPartitions);

	public void registerMapperTask(int jobID, JobConfiguration jobConf,
			HashMap<Integer, String> chunkSets);

}
