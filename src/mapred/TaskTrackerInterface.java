package mapred;

import java.io.IOException;
import java.rmi.Remote;
import java.util.ArrayList;
import java.util.HashMap;

public interface TaskTrackerInterface extends Remote {
	
	public byte[] getPartitionContent(String path) throws IOException;

	public void registerMapperTask(int jobID, JobConfiguration jobConf, HashMap<Integer, String> chunkSets) throws IOException;

	public void registerReduceTask(int jobID, int partitionNo, HashMap<String, ArrayList<String>> nodesWithPartitions, int numOfPartitions) throws IOException;
	
	public void remove(int jobID);
}
