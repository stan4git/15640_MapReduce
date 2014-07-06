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
import java.util.HashSet;
import java.util.Hashtable;
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
	private static NameNodeInterface nameNode = null;
	
	// several configuration files' paths : DFS/map reduce/ slaveList
	private static String DFSConfPath = "conf/dfs.conf";
	private static String MapReducePath = "conf/mapred.conf";
	private static String SlaveListPath = "conf/slaveList";
	
	// These 3 contains JobTracker's registry IP,registry port, service port and service name
	private static Integer jobTrackerPort;
	private static Integer jobTrackerRegPort;
	private static String jobTrackServiceName;
	
	// These 3 contains NameNode's registry IP,registry port and service name
	private static String nameNodeIP;
	private static Integer nameNodeRegPort;
	private static String nameNodeService;
	
	// The path for uploading the programmer's Mapper and Reducer
	private static String jobUploadPath;
	
	// Create a thread pool
	private static ExecutorService executor = Executors.newCachedThreadPool();
	
	// Global Job ID
	private static volatile Integer globaljobID = 0;
	
	// jobID -> All map tasks, jobID -> unfinished Map Tasks
	// jobID -> All reduce Tasks, jobID -> unfinished reduce tasks
	public static ConcurrentHashMap<Integer, HashMap<String, TaskStatusInfo>> jobID_node_taskStatus = new ConcurrentHashMap<Integer, HashMap<String, TaskStatusInfo>>();
	
	// node -> status
	public static HashMap<String, Boolean> node_status;
	public static ConcurrentHashMap<String, Integer> node_totalTasks = new ConcurrentHashMap<String, Integer>();
	
	// <jobID,<Do Job Node,<ChunkID,Chunk host Node>>>
	public static ConcurrentHashMap<Integer, HashMap<String, HashMap<Integer, String>>> jobID_mapTasks = new ConcurrentHashMap<Integer, HashMap<String, HashMap<Integer, String>>>();
	
	// Associate jobID with configuration information
	public static ConcurrentHashMap<Integer, KVPair> jobID_mapRedName = new ConcurrentHashMap<Integer, KVPair>();
	public static ConcurrentHashMap<Integer, KVPair> jobID_mapRedPath = new ConcurrentHashMap<Integer, KVPair>();
	
	protected JobTracker() throws RemoteException {
		super();
		globaljobID = 0;
	}
	
	@Override
	public String submitJob (JobConfiguration jobConf, KVPair mapper, KVPair reducer) {
		
		// step1 : find if the input file exists on the DFS system.
		try {
			Registry reigstry = LocateRegistry.getRegistry(nameNodeIP, nameNodeRegPort);
			nameNode = (NameNodeInterface)reigstry.lookup(nameNodeService);
			if(!nameNode.fileExist(jobConf.getInputfile())) {
				return "INPUTNOTFOUND";
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
			return "FAIL";
		}
		// step 2: update the globaljobID
		Integer jobID = globaljobID;
		updateGlobaljobID();
		
		// step 3: Get the working node and chunks from jobScheduler
		Hashtable<Integer,HashSet<String>> chunkDistribution = nameNode.getFileDistributionTable().get(jobConf.getInputfile());
		HashMap<String,HashMap<Integer,String>> nodeToChunks = jobScheduler.selectBestNodeToChunks(chunkDistribution);
		
		// step 4: copy the programmer's Mapper and Reducer into local directory
		if(nodeToChunks == null) {
			return "FAIL";
		} else {
			localizeJob(mapper, reducer, jobID);
		}
		
		// step 5: Send work to node 
		for (String node : nodeToChunks.keySet()) {
			if(jobID_node_taskStatus.get(jobID).get(node) == null) {
				HashMap<String, TaskStatusInfo> taskNodeStatus = new HashMap<String, TaskStatusInfo>();
				taskNodeStatus.put(node, new TaskStatusInfo());
				jobID_node_taskStatus.put(jobID, taskNodeStatus);
			}
			
			System.out.println("choose node: " + node + " to run one or more Mapper tasks!");
			TaskThread mapTask = new TaskThread(node,jobID,jobConf,nodeToChunks.get(node),true,0,null);
			executor.execute(mapTask);
		}
		return jobID.toString();
	}

	public void updateGlobaljobID(){
		synchronized(globaljobID){
			globaljobID++;
		}
	}
	
	@Override
	public void localizeJob (KVPair mapper, KVPair reducer, Integer jobID) { 
		
		// KVPair mapper
		// key: wordCount.wordMapper
		// value: wordCount/wordMapper.class
		String[] mappers =  ((String)mapper.getKey()).split(".");
		String[] reducers = ((String)reducer.getKey()).split(".");
		// mapperPath:/tmp/upload/wordMapper-0.class
		String mapperPath = jobUploadPath + mappers[mappers.length - 1] + "-" + jobID +".class";
		String reducerPath = jobUploadPath + reducers[reducers.length - 1] + "-" + jobID + ".class";
		
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
			// TODO Auto-generated method stub
			TaskThread reduceTask = new TaskThread(chosenReduceNodes.get(i), jobID, null, null, false, i, null);	
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
		String mapperClassName = (String)jobID_mapRedName.get(jobID).getKey();
		String mapperClassPath = (String)jobID_mapRedPath.get(jobID).getKey();
		return new KVPair(mapperClassName, IOUtil.readFile(mapperClassPath));
	}
	
	@Override
	public void terminateJob(int jobID) {
		jobID_node_taskStatus.remove(jobID);
		jobID_mapTasks.remove(jobID);
		jobID_mapRedName.remove(jobID);
		jobID_mapRedPath.remove(jobID);
	}

	@Override
	public JobStatus checkJobStatus(Integer jobID) {
		
		return null;
	}

	/**
	 * This method is used to calculate the percentage of finished reducer tasks.
	 * 
	 * @param jobID - the jobID of the reducer tasks      
	 * @return the percentage of finished reducer tasks  
	 */
	
	@Override
	public double getMapperProgress (Integer jobID) {
		int totalMapTasks = 0;
		int unfinishedMapTasks = 0;
		
		for(String node : jobID_node_taskStatus.get(jobID).keySet()) {
			totalMapTasks += jobID_node_taskStatus.get(jobID).get(node).getTotalMapTasks();
			unfinishedMapTasks += jobID_node_taskStatus.get(jobID).get(node).getUnfinishedMapTasks();
		}
		
		return 1 - (double)unfinishedMapTasks / (double)totalMapTasks;
	}

	/**
	 * This method is used to calculate the percentage of finished reducer tasks.
	 * 
	 * @param jobID - the jobID of the reducer tasks      
	 * @return the percentage of finished reducer tasks  
	 */
	
	@Override
	public double getReducerProgress (Integer jobID) {
		int totalReduceTasks = 0;
		int unfinishedReduceTasks = 0;
		
		for(String node : jobID_node_taskStatus.get(jobID).keySet()) {
			totalReduceTasks += jobID_node_taskStatus.get(jobID).get(node).getTotalReduceTasks();
			totalReduceTasks += jobID_node_taskStatus.get(jobID).get(node).getUnfinishedReduceTasks();
		}
		
		return 1 - (double)unfinishedReduceTasks / (double)totalReduceTasks;
	}
	
	
	@Override
	public void responseToHeartBeat(String node, ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus) {
		int unfinishedMapTasks = 0;
		int unfinishedReduceTasks = 0;
		
		for(int jobID : jobID_taskStatus.keySet()) {
			TaskStatusInfo taskStatusInfo = jobID_taskStatus.get(jobID);
			jobID_node_taskStatus.get(jobID).put(node, taskStatusInfo);
			unfinishedMapTasks += taskStatusInfo.getUnfinishedMapTasks();
			unfinishedReduceTasks += taskStatusInfo.getUnfinishedReduceTasks();
		}
		node_totalTasks.put(node, unfinishedMapTasks + unfinishedReduceTasks);
	}
	
	private void initSlaveNodes (String slaveListPath) {
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
	
	public static void main (String args[]) {
		try {
			JobTracker jobTracker = new JobTracker();
			
			// 1. Read DFS and MapReduce's configuration files and fill the properties to the jobTrack object
			IOUtil.readConf(DFSConfPath, jobTracker);
			IOUtil.readConf(MapReducePath, jobTracker);
			
			// 2. Initialize slave nodes
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

}
