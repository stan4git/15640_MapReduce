package mapred;

import java.rmi.registry.Registry;

import util.IOUtil;

/**
 * 1. runjob(file, mapper class, reducer class)
 * 2. submit job(passing all files to jobTracker, RMI)
 * 3. get job information from JobTracker (5 second)
 */
public class JobClient {

	// 1. JobTracker's host IP address
	private static String jobTrackerIP;
	// 2. JobTracker's registry port
	private static Integer jobTrackerRegPort;
	// 3. JobTracker registry service name
	private static String jobTrackServiceName;
	// 4. map reduce's configuration file path
	private static String mapredPath = "conf/mapred.conf";
	// 5. JobConfiguration instance
	private static JobConfiguration jobConfiguration;
	// 6. RMI's Registry instance
	private static Registry registry;
	// 7. Maximum failure times
	private static Integer jobMaxFailureThreshold;
	// 8. Job Id that get from JobTracker
	private static Integer jobId;
	
	public void runJob (JobConfiguration jobConf){
		JobClient jobClient = new JobClient();
		jobConfiguration = jobConf;
		IOUtil.readConf(mapredPath, jobClient);
	}
}
