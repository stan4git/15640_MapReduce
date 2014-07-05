package mapred;

import java.rmi.Remote;

import util.JobStatus;
import format.KVPair;

public interface JobTrackerInterface extends Remote {

	public String submitJob(JobConfiguration jobConf, KVPair mapper, KVPair reducer);

	public void localizeJob(KVPair mapper, KVPair reducer);
	
	public void terminateJob(Integer jobId);

	public JobStatus checkJobStatus(Integer jobId);

	public double checkMapper(Integer jobId);

	public double checkReducer(Integer jobId);

	public KVPair getMapperInfo(int jobID);
	
	public KVPair getReducerInfo(int jobID);

	public void startReducePhase (int jobID);
	
	
	
	
	
	
}
