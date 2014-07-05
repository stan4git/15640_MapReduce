package mapred;

import util.JobStatus;
import format.KVPair;

public interface JobTrackerInterface {

	public String submitJob(JobConfiguration jobConf, KVPair mapper, KVPair reducer);

	public void terminateJob(Integer jobId);

	public JobStatus checkJobStatus(Integer jobId);

	public double checkMapper(Integer jobId);

	public double checkReducer(Integer jobId);
	
}
