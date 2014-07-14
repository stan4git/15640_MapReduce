package mapred;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import format.KVPair;
import util.IOUtil;
import util.JobStatus;
import util.PathConfiguration;

/**
 * This class is designed to provide the user to initiate a job and monitor
 * the job's status. It just contains one method runJob(). This method contains
 * two main parts: 1) submit the job to the JobTracker and get the 
 * JobId; 2) Monitor this job's status.
 * 
 * This class also handle the tolerance of the job failure. The programmer can
 * set the jobMaxiFailureThreshold in the mapred.conf and the method will 
 * try the time you set in the configuration file.
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public class JobClient {

	// 1. JobTracker's host IP address
	private static String jobTrackerIP;
	// 2. JobTracker's registry port
	private static Integer jobTrackerRegPort;
	// 3. JobTracker registry service name
	private static String jobTrackServiceName;
	// 5. RMI's Registry instance
	private static Registry registry;
	// 6. Maximum failure times
	private static Integer jobMaxFailureThreshold;
	// 7. Job Id that get from JobTracker
	private static Integer jobId;
	// 8. actual failure time
	private static Integer failureTimes = 1;
	
	/**
	 * This method contains two parts: start the job and monitor the job's status
	 * 
	 * @param jobConf This object contains the basic info for running the Job
	 * @throws IOException 
	 */
	public void runJob (JobConfiguration jobConf) throws IOException{
		JobClient jobClient = new JobClient();
		IOUtil.readConf(PathConfiguration.MapReducePath, jobClient);
		JobTrackerInterface jobtracker = null;
		
		// Get the Remote Object Reference from JobTracker
		try {
			registry = LocateRegistry.getRegistry(jobTrackerIP, jobTrackerRegPort);
			jobtracker = (JobTrackerInterface)registry.lookup(jobTrackServiceName);
		} catch (RemoteException | NotBoundException e) {
			System.err.println("Failure happened when looking up the service!");
			e.printStackTrace();
			System.exit(-1);
		}
		
		// Submit the job first time with the necessary 
		String mapperName = jobConf.getMapperClass().getName().replace('.', '/');
		String reducerName = jobConf.getReducerClass().getName().replace('.', '/');
		KVPair mapper = new KVPair(jobConf.getMapperClass().getName(),mapperName + ".class");
		KVPair reducer = new KVPair(jobConf.getReducerClass().getName(),reducerName + ".class");
		String res = null;
		res = jobtracker.submitJob(jobConf,mapper,reducer);
		
		// Failure handling
		if(res.equals("INPUTNOTFOUND")){
			System.err.println("The input file cannot be found in the DFS!");
			System.exit(-1);
		}
		
		while(res.equals("FAIL")){
			System.err.println("Job failed!");
			if(failureTimes < jobMaxFailureThreshold) {
				System.out.println("Restarting job!");
				res = jobtracker.submitJob(jobConf,mapper,reducer);
				failureTimes++;
				continue;
			} else {
				jobtracker.terminateJob(jobId);
				System.out.println("Job terminated!");
				System.exit(-1);
			}
		}
		
		jobId = Integer.parseInt(res);
		
		// Monitoring
		while(true) {
			JobStatus status = jobtracker.checkJobStatus(jobId);
			if(status.equals("SUCCESS")) {
				System.out.println("Your job has been executed successfully!");
				System.out.println("Your jobId is "+jobId+" and your outputfile name is " + jobConf.getOutputfile());
				System.out.println("The actual output format is [jobId]-[outputfilename]-part-[partitionNumber]");
				jobtracker.terminateJob(jobId);
				break;
			} else if(status.equals("INPROGRESS")) {
				double mapperPercentage = jobtracker.getMapperProgress(jobId);
				double reducePercentage = jobtracker.getReducerProgress(jobId);
				System.out.printf("Mapper: %fpercent; Reducer: %fpercent\n", mapperPercentage*100, reducePercentage*100);
			} else if(status.equals("FAIL")) {
				System.err.println("Job failed!");
				if(failureTimes < jobMaxFailureThreshold) {
					System.out.println("Restarting job!");
					res = jobtracker.submitJob(jobConf,mapper,reducer);
					if(!res.equals("FAIL") &&  !res.equals("INPUTNOTFOUND")){
						jobId = Integer.parseInt(res);
					}
					failureTimes++;
					continue;
				} else {
					jobtracker.terminateJob(jobId);
					System.out.println("Job terminated!");
					break;
				}
			}
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				System.err.println("Exception happened when monitoring!");
				e.printStackTrace();
			}
		}
	}
}
