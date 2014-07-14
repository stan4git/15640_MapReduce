package mapred;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import util.JobStatus;
import format.KVPair;

public interface JobTrackerInterface extends Remote {

	public String submitJob(JobConfiguration jobConf, KVPair mapper, KVPair reducer) throws IOException;

	public void localizeJob (KVPair mapper, KVPair reducer, Integer jobID) throws IOException;
	
	public void terminateJob(int jobID) throws RemoteException;

	public JobStatus checkJobStatus(Integer jobId) throws RemoteException;

	public double getMapperProgress(Integer jobId) throws RemoteException;

	public double getReducerProgress(Integer jobId) throws RemoteException;

	public KVPair getMapperInfo(int jobID) throws IOException;
	
	public KVPair getReducerInfo(int jobID) throws IOException;

	public void startReducePhase(int jobID) throws RemoteException;

	public void notifyMapperFinish(String node, ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus, 
			ConcurrentHashMap<Integer, ArrayList<String>> jobID_parFilePath) throws RemoteException;
	
	public void notifyReducerFinish (String node, ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus) throws RemoteException;
	
	public void responseToHeartbeat (String node, ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus) throws RemoteException;

	public void updateJobStatus(Integer jobId, JobStatus jobStatus) throws RemoteException;
}
