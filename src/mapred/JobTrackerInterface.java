package mapred;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import util.JobStatus;
import format.KVPair;
/**
 * This class is a interface of JobTracker which is a remote object
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public interface JobTrackerInterface extends Remote {
	/**
	 * This method is used to submit job
	 * @param jobConf the programmer's configuration about the job
	 * @param mapper Mapper class and name
	 * @param reducer Reducer class and name
	 * @return String message from Server including "INPUTNOTFOUND", "FAIL" or jobId
	 * @throws IOException
	 */
	public String submitJob(JobConfiguration jobConf, KVPair mapper, KVPair reducer) throws IOException;

	/**
	 * This method is used to upload the mapper and reduce class into the server
	 * @param mapper Mapper class and name
	 * @param reducer Reducer class and name
	 * @param jobID Job ID
	 * @throws IOException
	 */
	public void localizeJob (KVPair mapper, KVPair reducer, Integer jobID) throws IOException;
	/**
	 * This method is used to terminate the specific job
	 * @param jobID Job ID
	 * @throws RemoteException
	 */
	public void terminateJob(int jobID) throws RemoteException;
	/**
	 * This method is used to check the Job's status
	 * @param jobId Job ID
	 * @return JobStatus including FAIL, SUCCESS, INPROGRESS
	 * @throws RemoteException
	 */
	public JobStatus checkJobStatus(Integer jobId) throws RemoteException;
	/**
	 * This method is used to calculate the percentage of finished reducer tasks.
	 * 
	 * @param jobId - the jobID of the reducer tasks      
	 * @return the percentage of finished reducer tasks  
	 * 
	 * @throws RemoteException
	 */
	public double getMapperProgress(Integer jobId) throws RemoteException;
	/**
	 * This method is used to calculate the percentage of finished reducer tasks.
	 * 
	 * @param jobId - the jobID of the reducer tasks      
	 * @return the percentage of finished reducer tasks  
	 */
	public double getReducerProgress(Integer jobId) throws RemoteException;
	/**
	 * This method is used to get the Mapper Info according to the jobID
	 * @param jobID job ID
	 * @return the KVPair including the name of the Job ID's Mapper class and name
	 * @throws IOException
	 */
	public KVPair getMapperInfo(int jobID) throws IOException;
	/**
	 * This method is used to get the Reducer Info according to the jobID
	 * @param jobID job ID
	 * @return the KVPair including the name of the Job ID's Reducer class and name 
	 * @throws IOException
	 */
	public KVPair getReducerInfo(int jobID) throws IOException;
	/**
	 * This method initiates the Reduce work
	 * @param jobID job ID 
	 * @throws RemoteException
	 */
	public void startReducePhase(int jobID) throws RemoteException;
	/**
	 * This method is used to notify the jobTracker that all the mapper workers
	 * has done the job and their generating files
	 * @param node  The node handling the mapper task
	 * @param jobID_taskStatus Task status info
	 * @param jobID_parFilePath the Partition files
	 * @param jobID
	 * @throws RemoteException
	 */
	public void notifyMapperFinish(String node, ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus, 
			ConcurrentHashMap<Integer, ArrayList<String>> jobID_parFilePath, int jobID) throws RemoteException;
	/**
	 * This method is used to notify the jobTracker that all the reducer workers
	 * has done and they can change the related information
	 * @param node The node handled the reducer task
	 * @param jobID_taskStatus JobId and task status
	 * @throws RemoteException
	 */
	public void notifyReducerFinish (String node, ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus) throws RemoteException;
	/**
	 * This method is used to update the job status
	 * @param jobId job ID
	 * @param jobStatus job Status
	 * @throws RemoteException
	 */
	public void updateJobStatus(Integer jobId, JobStatus jobStatus) throws RemoteException;
	/**
	 * This method is used to get the output file name
	 * @param jobID
	 * @return get the output file name
	 */
	public String getOutputFileName(int jobID) throws RemoteException;
	/**
	 * This method is used to register the task tracker IP
	 * @param taskTrackerIP
	 * @throws RemoteException
	 */
	public void registerTaskTracker(String taskTrackerIP) throws RemoteException;
	/**
	 * This method is used to handle the situation if some task node turns down,
	 * the system need to recover all the mappers and reducers on the down node.
	 * @param node
	 * @throws RemoteException
	 */
	public void handleNodeFailure (String node) throws RemoteException;
}
