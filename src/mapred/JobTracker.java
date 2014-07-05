package mapred;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import dfs.NameNodeInterface;
import util.IOUtil;
import util.JobStatus;
import format.KVPair;

public class JobTracker extends UnicastRemoteObject implements JobTrackerInterface {

	private static final long serialVersionUID = 9023603070698668607L;
	private static JobScheduler jobScheduler = new JobScheduler();
	
	// several configuration files' paths : DFS/map reduce/ slaveList
	private static String DFSConfPath = "conf/dfs.conf";
	private static String MapReducePath = "conf/mapred.conf";
	private static String SlaveListPath = "conf/slaveList";
	
	/*These 3 contains JobTracker's registry IP,registry port, service port and service name*/
	private static Integer jobTrackerPort;
	private static Integer jobTrackerRegPort;
	private static String jobTrackServiceName;
	
	// The path for uploading the programmer's Mapper and Reducer
	private static String jobUploadPath;
	// The Mapper's final partition numbers
	private static Integer partitionNums;
	
	// Create a thread pool
	private static ExecutorService executor = Executors.newCachedThreadPool();
	
	// Global Job ID
	private static volatile Integer jobID = 0;
	
	// JobId -> All map tasks, JobId -> unfinished Map Tasks, 
	// JobId -> All reduce Tasks, JobId -> unfinished reduce tasks
	public static ConcurrentHashMap<Integer, Integer> jobID_totalMapTasks = new ConcurrentHashMap<Integer, Integer>();
	public static ConcurrentHashMap<Integer, Integer> jobID_unfinishedMapTasks = new ConcurrentHashMap<Integer, Integer>();
	public static ConcurrentHashMap<Integer, Integer> jobID_totalReduceTasks = new ConcurrentHashMap<Integer, Integer>();
	public static ConcurrentHashMap<Integer, Integer> jobID_unfinishedReduceTasks = new ConcurrentHashMap<Integer, Integer>();
	
	/* Associate Node with its tasks */
	public static HashMap<String, Boolean> node_status;
	public static ConcurrentHashMap<String, Integer> node_totalTasks = new ConcurrentHashMap<String, Integer>();
//	public static ConcurrentHashMap<String, Integer> node_mapTasks = new ConcurrentHashMap<String, Integer>();
//	public static ConcurrentHashMap<String, Integer> node_reduceTasks = new ConcurrentHashMap<String, Integer>();
	
	//<JobID,<Do Job Node,<ChunkID,Chunk host Node>>>
	public static ConcurrentHashMap<Integer, HashMap<String, HashMap<Integer, String>>> jobID_mapTasks = new ConcurrentHashMap<Integer, HashMap<String, HashMap<Integer, String>>>();
	
	/* Associate jobID with configuration information */
	public static ConcurrentHashMap<Integer, KVPair> jobID_mapRedName = new ConcurrentHashMap<Integer, KVPair>();
	public static ConcurrentHashMap<Integer, KVPair> jobID_mapRedPath = new ConcurrentHashMap<Integer, KVPair>();
	
	
	
	protected JobTracker() throws RemoteException {
		super();
		jobID = 0;
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
	public void startReducePhase (int jobID) {
		
		System.out.println("Start reduce job !!");
		int numOfPartitions = 0;
		ArrayList<String> chosenReduceNodes = jobScheduler.pickBestNodesForReduce(numOfPartitions);
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
		return null;
	}
	
	@Override
	public void terminateJob(Integer jobId) {
		
	}
	
	public static void main (String args[]) {
		try {
			JobTracker jobTracker = new JobTracker();
			
			// 1. Read DFS and MapReduce's configuration files and fill the properties to the jobTrack object
			IOUtil.readConf(DFSConfPath, jobTracker);
			IOUtil.readConf(MapReducePath, jobTracker);
			
			// 2. Initialize the Slave Node's tasks
			jobTracker.initSlaveNodes(SlaveListPath);
			
			// 3. Build the RMI Registry Server and bind the service to the registry server
			JobTrackerInterface stub = (JobTrackerInterface) exportObject (jobTracker, jobTrackerPort);
			Registry registry = LocateRegistry.createRegistry(jobTrackerRegPort);
			registry.rebind(jobTrackServiceName, stub);
			InetAddress address = InetAddress.getLocalHost();
			System.out.println("The JobTracker's IP address is " + address.getHostAddress());
			System.out.println("The JobTracker has started successfully!");
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public void initSlaveNodes(String slaveListPath) {
		try {
			String content = new String(IOUtil.readFile(slaveListPath),"UTF-8");
			String[] lines = content.split("\n");
			for(int i = 0; i < lines.length; i++) {
				node_totalTasks.put(lines[i],0);
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	@Override
	public JobStatus checkJobStatus(Integer jobId) {
		return null;
	}

	@Override
	public double checkMapper(Integer jobId) {
		return 0;
	}

	@Override
	public double checkReducer(Integer jobId) {
		return 0;
	}

}
