package mapred;

import java.io.IOException;
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

import dfs.DFSClient;
import dfs.NameNodeInterface;
import format.KVPair;
import util.IOUtil;
import util.JobStatus;
import util.PathConfiguration;

/**
 * This class is used to initialize the Task Tracker which was used to monitor
 * the tasks running on the node and communicate with the JobTracker server.
 * 
 * @author menglonghe
 * @author sidilin
 * 
 */
public class TaskTracker extends UnicastRemoteObject implements
		TaskTrackerInterface {

	private static final long serialVersionUID = -897603125687983899L;

	private static JobTrackerInterface jobTracker = null;
	private static NameNodeInterface nameNode = null;
	private static TaskTracker taskTracker = null;
	private static DFSClient dfsClient = null;

	/** Create a thread pool */
	private static ExecutorService executor = Executors.newCachedThreadPool();
	/** jobID - > partition files path */
	public static ConcurrentHashMap<Integer, ArrayList<String>> jobID_parFilePath = new ConcurrentHashMap<Integer, ArrayList<String>>();
	/** jobID - > Task status */
	public static ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus = new ConcurrentHashMap<Integer, TaskStatusInfo>();
	/** <jobID,<node,List<map ID>> */
	public static ConcurrentHashMap<Integer, HashMap<String, ArrayList<Integer>>> jobID_node_mapID = new ConcurrentHashMap<Integer, HashMap<String, ArrayList<Integer>>>();
	/** reducer and mapper class's name */
	private static String reducerClassName;
	private static String mapperClassName;

	/** dataNode's registry port and service */
	private static Integer dataNodeRegPort;
	private static String dataNodeService;

	/** partition numbers */
	private static Integer partitionNums;

	/** partition file's path */
	private static String partitionFilePath;

	/** namenode's registry port, IP and service name */
	private static String nameNodeIP;
	private static Integer nameNodeRegPort;
	private static String nameNodeService;

	/** jobTracker's IP, registry port and service name */
	private static String jobTrackerIP;
	private static Integer jobTrackerRegPort;
	private static String jobTrackServiceName;

	/** TaskTracker's service port, registry port and service name */
	private static Integer taskTrackerPort;
	private static Integer taskTrackerRegPort;
	private static String taskTrackServiceName;

	/** the number of chunks of each mapper can handle */
	private static Integer mapperChunkThreshold;
	/** the maximum failure times of one job */
	private static Integer jobMaxFailureThreshold;
	/** the reduce file path */
	private static String reduceResultPath;
	/** map result temporary path */
	private static String mapResTemporaryPath;
	/** client Port for task Tracker */
	private static Integer clientPortForTaskTrack;
	/** client Registry port for task Tracker */
	private static Integer clientRegPortForTaskTrack;

	private static String node;

	/** default constructor */
	protected TaskTracker() throws RemoteException {
		super();
	}

	@Override
	public void registerMapperTask(int jobID, JobConfiguration jobConf,
			HashMap<Integer, String> chunkSets) throws RemoteException {
		System.out.println("This node need to handle the chunk number is: "
				+ chunkSets.size());
		int count = 0, mapNums = 0;
		Hashtable<Integer, ArrayList<KVPair>> mappers = new Hashtable<Integer, ArrayList<KVPair>>();
		ArrayList<KVPair> pairs = null;
		// step1: split of the chunks into the different map workers
		for (Integer chunkNum : chunkSets.keySet()) {
			if (count == mapperChunkThreshold) {
				mappers.put(mapNums, pairs);
				count = 0;
				mapNums++;
			}
			if (count == 0) {
				pairs = new ArrayList<KVPair>();
			}
			KVPair pair = new KVPair(chunkNum, chunkSets.get(chunkNum));
			pairs.add(pair);
			count++;
		}
		if (count != 0) {
			mappers.put(mapNums, pairs);
			mapNums++;
		}

		System.out.println("It needs " + mapNums + " map worker!");
		// step2: update some related info
		TaskStatusInfo taskStatusInfo;
		if (jobID_taskStatus.containsKey(jobID)) {
			taskStatusInfo = jobID_taskStatus.get(jobID);
		} else {
			taskStatusInfo = new TaskStatusInfo();
		}
		taskStatusInfo.setTotalMapTasks(taskStatusInfo.getTotalMapTasks()
				+ mapNums);
		taskStatusInfo.setUnfinishedMapTasks(taskStatusInfo
				.getUnfinishedMapTasks() + mapNums);
		jobID_taskStatus.put(jobID, taskStatusInfo);

		// step3: download the Mapper class from the jobTracker
		try {
			localizeMapTask(jobID);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// step4: start different map workers according to the table
		for (Integer mapperNum : mappers.keySet()) {
			ArrayList<KVPair> chunksAndNodes = mappers.get(mapperNum);
			String mapperName = null;
			try {
				mapperName = jobTracker.getMapperInfo(jobID).getKey()
						.toString();
			} catch (IOException e) {
				e.printStackTrace();
			}
			RMIServiceInfo rmiServiceInfo = new RMIServiceInfo();
			rmiServiceInfo.settingForMapper(dataNodeRegPort, dataNodeService,
					partitionNums, partitionFilePath);
			startMapTask(jobID, chunksAndNodes.size(), jobConf, chunksAndNodes,
					mapperName, mapperNum, rmiServiceInfo, 1);
		}
	}

	/**
	 * this method is used to download and write the mapper task into local disk
	 * 
	 * @param jobID
	 * @throws IOException
	 */
	public void localizeMapTask(int jobID) throws IOException {
		KVPair mapInfo = jobTracker.getMapperInfo(jobID);
		mapperClassName = mapInfo.getKey().toString().replace('.', '/')
				+ ".class";
		byte[] mapperClassContent = mapInfo.getValue().toString().getBytes();
		IOUtil.writeBinary(mapperClassContent, mapperClassName);
	}

	/**
	 * This method is used to start the map task using the Thread pool to load
	 * the mapper Runner
	 * 
	 * @param jobID
	 * @param numOfChunks
	 * @param jobConf
	 * @param pairLists
	 * @param classname
	 * @param mapperNum
	 * @param rmiServiceInfo
	 * @param tryNums
	 */
	public void startMapTask(int jobID, int numOfChunks,
			JobConfiguration jobConf, ArrayList<KVPair> pairLists,
			String classname, Integer mapperNum, RMIServiceInfo rmiServiceInfo,
			int tryNums) {

		MapRunner mapRunner = new MapRunner(jobID, numOfChunks, jobConf,
				pairLists, classname, mapperNum, rmiServiceInfo, 1);
		executor.execute(mapRunner);
	}

	@Override
	public void registerReduceTask(int jobID, int partitionNo,
			HashMap<String, ArrayList<String>> nodesWithPartitions,
			int numOfPartitions) throws RemoteException {
		// step1: Update task status
		TaskStatusInfo taskStatusInfo;
		if (jobID_taskStatus.containsKey(jobID)) {
			taskStatusInfo = jobID_taskStatus.get(jobID);
		} else {
			taskStatusInfo = new TaskStatusInfo();
		}
		taskStatusInfo.setTotalReduceTasks(taskStatusInfo.getTotalReduceTasks()
				+ numOfPartitions);
		taskStatusInfo.setUnfinishedReduceTasks(taskStatusInfo
				.getUnfinishedReduceTasks() + numOfPartitions);
		jobID_taskStatus.put(jobID, taskStatusInfo);
		// step2: download the Reducer class from the jobTracker
		try {
			localizeReduceTask(jobID);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// step3: start the reduce work
		String reduceName = null;
		try {
			reduceName = jobTracker.getReducerInfo(jobID).getKey().toString();
		} catch (IOException e) {
			e.printStackTrace();
		}
		RMIServiceInfo rmiServiceInfo = new RMIServiceInfo();
		rmiServiceInfo.settingForReducer(taskTrackerRegPort,
				taskTrackServiceName);
		String outputFileName = jobTracker.getOutputFileName(jobID);
		
		startReduceTask(jobID, partitionNo, nodesWithPartitions,
				reduceName, rmiServiceInfo, reduceResultPath,mapResTemporaryPath,outputFileName,dfsClient);
	}

	/**
	 * This method is used to download the Reducer class from the jobTracker
	 * 
	 * @param jobID
	 * @throws IOException
	 */
	public void localizeReduceTask(int jobID) throws IOException {
		KVPair reducerInfo = jobTracker.getReducerInfo(jobID);
		reducerClassName = reducerInfo.getKey().toString().replace('.', '/')
				+ ".class";
		byte[] reducerClassContent = reducerInfo.getValue().toString().getBytes();
		IOUtil.writeBinary(reducerClassContent, reducerClassName);
	}

	/**
	 * This method is used to start the reduce task using the Thread pool to
	 * load the Reduce Runner
	 * 
	 * @param jobID
	 * @param partitionNo
	 * @param nodesWithPartitions
	 * @param className
	 * @param rmiServiceInfo
	 * @param reduceResultPath
	 */
	public void startReduceTask(int jobID, int partitionNo,
			HashMap<String, ArrayList<String>> nodesWithPartitions,
			String className, RMIServiceInfo rmiServiceInfo,String reduceResultPath,
			 String mapResTemporaryPath, String outputFileName, DFSClient dfsClient) {
		ReduceRunner reduceRunner = new ReduceRunner(jobID, partitionNo,
				nodesWithPartitions, className, rmiServiceInfo,
				reduceResultPath,mapResTemporaryPath, outputFileName,dfsClient);
		executor.execute(reduceRunner);
	}

	@Override
	public byte[] getPartitionContent(String path) throws RemoteException,
			IOException {
		return IOUtil.readFile(path);
	}

	/**
	 * This method is used to update the mapper status
	 * 
	 * @param jobID
	 * @param isSuccessful
	 * @throws RemoteException
	 */
	public static void updateMapStatus(Integer jobID, boolean isSuccessful)
			throws RemoteException {
		if (isSuccessful) {
			TaskStatusInfo taskStatusInfo = TaskTracker.jobID_taskStatus
					.get(jobID);
			int curUnfinishedMapTasks = taskStatusInfo.getUnfinishedMapTasks() - 1;
			taskStatusInfo.setUnfinishedMapTasks(curUnfinishedMapTasks);
			TaskTracker.jobID_taskStatus.put(jobID, taskStatusInfo);

			if (curUnfinishedMapTasks == 0) {
				jobTracker.notifyMapperFinish(node, jobID_taskStatus,jobID_parFilePath, jobID);
			}
		}
	}

	/**
	 * This method is used to update the reducer's status
	 * 
	 * @param jobID
	 * @param isSuccessful
	 */
	public static void updateReduceStatus(Integer jobID, boolean isSuccessful) {
		if (isSuccessful) {
			TaskStatusInfo taskStatusInfo = TaskTracker.jobID_taskStatus
					.get(jobID);
			int curUnfinishedReduceTasks = taskStatusInfo.getUnfinishedReduceTasks() - 1;
			taskStatusInfo.setUnfinishedReduceTasks(curUnfinishedReduceTasks);
			TaskTracker.jobID_taskStatus.put(jobID, taskStatusInfo);

			if (curUnfinishedReduceTasks == 0) {
				try {
					jobTracker.notifyReducerFinish(node, jobID_taskStatus);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * This method is used to update the Job ID and related file paths
	 * 
	 * @param jobID
	 * @param filePaths
	 */
	public static void updateFilePaths(Integer jobID,
			ArrayList<String> filePaths) {
		ArrayList<String> parFilePath = null;
		if (!jobID_parFilePath.containsKey(jobID)) {
			parFilePath = new ArrayList<String>();
		} else {
			parFilePath = jobID_parFilePath.get(jobID);
		}
		parFilePath.addAll(filePaths);
		jobID_parFilePath.put(jobID, parFilePath);
	}

	/**
	 * This method is used to send the heartbeat to the jobTracker to update the
	 * info in real-time
	 */
	public ConcurrentHashMap<Integer, TaskStatusInfo>  heartBeat() throws RemoteException{
		//System.out.println("Sending task progress to JobTracker...");
		return jobID_taskStatus;
	}

	/**
	 * This method is used to handle the failure about the situation: the node
	 * is down. It redistribute the chunks and restart some new threads to run
	 * the work to the end. It at most run the times set in the configuration
	 * files.
	 * 
	 * @param jobID
	 * @param numOfChunks
	 * @param jobConf
	 * @param pairLists
	 * @param classname
	 * @param mapperNum
	 * @param rmiServiceInfo
	 * @param tryNums
	 * @throws RemoteException
	 */
	public static void handleDataNodeFailure(Integer jobID,
			Integer numOfChunks, JobConfiguration jobConf,
			ArrayList<KVPair> pairLists, String classname, Integer mapperNum,
			RMIServiceInfo rmiServiceInfo, int tryNums) throws RemoteException {

		if (tryNums < jobMaxFailureThreshold) {
			ArrayList<Integer> chunks = new ArrayList<Integer>();
			ArrayList<KVPair> pairs = new ArrayList<KVPair>();

			for (KVPair kvPair : pairLists) {
				chunks.add((Integer) kvPair.getKey());
			}
			Hashtable<Integer, HashSet<String>> chunkDistribution = nameNode
					.getFileDistributionTable().get(jobConf.getInputfile());
			HashSet<String> healthyNodes = nameNode.getHealthyNodes();

			for (int i = 0; i < chunks.size(); i++) {
				if (healthyNodes.contains((String) pairLists.get(i).getValue())) {
					continue;
				}
				HashSet<String> distribution = chunkDistribution.get(chunks
						.get(i));
				for (String node : distribution) {
					if (healthyNodes.contains(node)) {
						pairs.add(new KVPair(chunks.get(i), node));
					}
					break;
				}
			}
			taskTracker.startMapTask(jobID, numOfChunks, jobConf, pairs,
					classname, mapperNum, rmiServiceInfo, tryNums++);
		} else {
			jobTracker.updateJobStatus(jobID, JobStatus.FAIL);
			jobTracker.terminateJob(jobID);
			System.out.println("Job terminated!");
			System.exit(-1);
		}

	}

	/**
	 * This method is used to handle the map work node is down just terminate
	 * all the information about the job
	 * 
	 * @param jobID
	 */
	public static void handleMapperNodeFailure(int jobID) {
		try {
			jobTracker.terminateJob(jobID);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method is used to clear some info when some job is down
	 */
	public void remove(int jobID) {
		jobID_parFilePath.remove(jobID);
		jobID_taskStatus.remove(jobID);
		jobID_node_mapID.remove(jobID);
	}

	public static void main(String args[]) throws IOException {
		try {
			taskTracker = new TaskTracker();
			IOUtil.readConf(PathConfiguration.MapReducePath, taskTracker);
			IOUtil.readConf(PathConfiguration.DFSConfPath, taskTracker);

			try {
				Registry registry = LocateRegistry.getRegistry(jobTrackerIP,
						jobTrackerRegPort);
				jobTracker = (JobTrackerInterface) registry
						.lookup(jobTrackServiceName);

				Registry nameNodeRegistry = LocateRegistry.getRegistry(
						nameNodeIP, nameNodeRegPort);
				nameNode = (NameNodeInterface) nameNodeRegistry
						.lookup(nameNodeService);
				
				dfsClient = new DFSClient(clientPortForTaskTrack,clientRegPortForTaskTrack);
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}

			unexportObject(taskTracker, false);
			TaskTrackerInterface stub = (TaskTrackerInterface) exportObject(
					taskTracker, taskTrackerPort);
			Registry registry = LocateRegistry
					.createRegistry(taskTrackerRegPort);
			registry.rebind(taskTrackServiceName, stub);

			node = InetAddress.getLocalHost().getHostAddress();
			jobTracker.registerTaskTracker(node);
			System.out.println("I'm the TaskTracker for node " + node);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
}
