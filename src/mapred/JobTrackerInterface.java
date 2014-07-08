package mapred;

import java.rmi.Remote;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import util.JobStatus;
import format.KVPair;

public interface JobTrackerInterface extends Remote {

	public String submitJob(JobConfiguration jobConf, KVPair mapper, KVPair reducer);

	public void localizeJob (KVPair mapper, KVPair reducer, Integer jobID);
	
	public void terminateJob(int jobID);

	public JobStatus checkJobStatus(Integer jobId);

	public double getMapperProgress(Integer jobId);

	public double getReducerProgress(Integer jobId);

	public KVPair getMapperInfo(int jobID);
	
	public KVPair getReducerInfo(int jobID);

	public void startReducePhase(int jobID);

	public void notifyMapperFinish(String node, ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus, 
			ConcurrentHashMap<Integer, ArrayList<String>> jobID_parFilePath);
	
	public void notifyReducerFinish (String node, ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus);
	
	public void responseToHeartbeat (String node, ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus);

	public void updateJobStatus(Integer jobId, JobStatus jobStatus);
}
