package mapred;

import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import util.IOUtil;
import util.JobStatus;
import format.KVPair;

/**
 * 1. submit job
 * 2. 
 */
public class JobTracker extends UnicastRemoteObject implements JobTrackerInterface {
	
	/**
	 * @throws RemoteException
	 */
	
	private static JobScheduler jobScheduler = null;
	
	private static Integer jobTrackerPort = 1234;
	private static Integer jobTrackerRegPort = 5678;
	private static String jobTrackServiceName = "hi";
	
	private static ExecutorService executor = Executors.newCachedThreadPool();
	
	private static volatile Integer jobID = 0;
	
	/* Associate jobID with its tasks */
	public static ConcurrentHashMap<Integer, Integer> jobID_totalMapTasks = new ConcurrentHashMap<Integer, Integer>();
	public static ConcurrentHashMap<Integer, Integer> jobID_unfinishedMapTasks = new ConcurrentHashMap<Integer, Integer>();
	public static ConcurrentHashMap<Integer, Integer> jobID_totalReduceTasks = new ConcurrentHashMap<Integer, Integer>();
	public static ConcurrentHashMap<Integer, Integer> jobID_unfinishedReduceTasks = new ConcurrentHashMap<Integer, Integer>();
	
	/* Associate jobID with node, chunkNo and the origin node of chunk */
	public static ConcurrentHashMap<Integer, HashMap<String, HashMap<Integer, String>>> jobID_mapTasks = new ConcurrentHashMap<Integer, HashMap<String, HashMap<Integer, String>>>();
	
	/* Associate Node with its tasks */
	public static HashMap<String, Boolean> node_status;
	public static ConcurrentHashMap<String, Integer> node_totalTasks = new ConcurrentHashMap<String, Integer>();
	public static ConcurrentHashMap<String, HashSet<Integer>> node_mapTasks = new ConcurrentHashMap<String, HashSet<Integer>>();
	public static ConcurrentHashMap<String, HashSet<Integer>> node_reduceTasks = new ConcurrentHashMap<String, HashSet<Integer>>();
	
	/* Associate jobID with configuration information */
	public static ConcurrentHashMap<Integer, KVPair> jobID_mapRedName = new ConcurrentHashMap<Integer, KVPair>();
	public static ConcurrentHashMap<Integer, KVPair> jobID_mapRedPath = new ConcurrentHashMap<Integer, KVPair>();
	
	
	
	protected JobTracker() throws RemoteException {
		super();
	}
	
	@Override
	public String submitJob (JobConfiguration jobConf, KVPair mapper, KVPair reducer) {
		
		localizeJob(mapper, reducer);
		startReducePhase(jobID);
		return jobID.toString();
	}
	
	@Override
	public void localizeJob (KVPair mapper, KVPair reducer) { 
		
		// KVPair mapper
		// key: wordCount.wordMapper 
		// value: wordCount/wordMapper.class
		
		String mapperPath = jobID + (String)mapper.getKey() + ".class";
		String reducerPath = jobID + (String)reducer.getKey() + ".class";
		
		IOUtil.writeBinary((byte[])mapper.getValue(), mapperPath);
		IOUtil.writeBinary((byte[])reducer.getValue(), reducerPath);
		
		KVPair mapRedName = new KVPair ((String)mapper.getKey(), (String)reducer.getKey());
		KVPair mapRedPath = new KVPair (mapperPath, reducerPath);
		jobID_mapRedName.put(jobID, mapRedName);
		jobID_mapRedPath.put(jobID, mapRedPath);
		
	}
	
	@Override
	public JobStatus checkJobStatus(Integer jobId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double checkMapper(Integer jobId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double checkReducer(Integer jobId) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
	
	@Override
	public void startReducePhase (int jobID) {
		
		System.out.println("Start reduce job !!");
		int numOfPartitions = 0;
		ArrayList<String> chosenReduceNodes = JobScheduler.pickBestNodesForReduce(numOfPartitions);
		if(chosenReduceNodes == null) {
			System.out.println("System is busy, the job fails");
			return;
		}
		for(int i = 0; i < numOfPartitions; i++) {
			TaskThread reduceTask = new TaskThread();	
			executor.execute(reduceTask);
		}
	}
	
	@Override
	public KVPair getReducerInfo (int jobID) {
		String reducerClassName = (String)jobID_mapRedName.get(jobID).getValue();
		String reducerClassPath = (String)jobID_mapRedPath.get(jobID).getValue();
		return new KVPair(reducerClassName, IOUtil.readFile(reducerClassPath));
	}
	
	@Override
	public KVPair getMapperInfo(int jobID) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void terminateJob(Integer jobId) {
		// TODO Auto-generated method stub
		
	}
	
	public static void main (String args[]) {
		try {
			JobTracker jobTracker = new JobTracker();
			JobTrackerInterface stub = (JobTrackerInterface) exportObject (jobTracker, jobTrackerPort);
			Registry registry = LocateRegistry.createRegistry(jobTrackerRegPort);
			registry.rebind(jobTrackServiceName, stub);
			jobScheduler = new JobScheduler();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

}
