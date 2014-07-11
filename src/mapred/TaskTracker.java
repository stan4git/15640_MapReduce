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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dfs.NameNodeInterface;
import format.KVPair;
import util.IOUtil;
import util.JobStatus;

public class TaskTracker extends UnicastRemoteObject implements
		TaskTrackerInterface {

	private static final long serialVersionUID = -897603125687983899L;

	private static JobTrackerInterface jobTracker = null;
	private static NameNodeInterface nameNode = null;
	private static TaskTracker taskTracker = null;
	
	// Create a thread pool
	private static ExecutorService executor = Executors.newCachedThreadPool();

	private static final String mapredConf = "conf/mapred.conf";
	private static final String dfsConf = "conf/dfs.conf";

	public static ConcurrentHashMap<Integer, ArrayList<String>> jobID_parFilePath = new ConcurrentHashMap<Integer, ArrayList<String>>();
	public static ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus = new ConcurrentHashMap<Integer, TaskStatusInfo>();
	public static ConcurrentHashMap<Integer, HashMap<String, ArrayList<Integer>>> jobID_node_mapID = new ConcurrentHashMap<Integer, HashMap<String, ArrayList<Integer>>>();
	
	private static String reducerClassName;
	private static String mapperClassName;
	private static Integer dataNodeRegPort;
	private static String dataNodeService;
	private static Integer partitionNums;
	private static String partitionFilePath;
	private static String nameNodeIP;
	private static Integer nameNodeRegPort;
	private static String nameNodeService;
	private static String jobTrackerIP;
	private static Integer jobTrackerRegPort;
	private static String jobTrackServiceName;
	private static Integer taskPort;
	private static Integer taskTrackerRegPort;
	private static String taskTrackServiceName;
	private static Integer mapperChunkThreshold;
	private static Integer jobMaxFailureThreshold;
	
	private static String node;

	protected TaskTracker() throws RemoteException {
		super();

		try {
			Registry registry = LocateRegistry.getRegistry(jobTrackerIP, jobTrackerRegPort);
			jobTracker = (JobTrackerInterface) registry.lookup(jobTrackServiceName);
			
			Registry nameNodeRegistry = LocateRegistry.getRegistry(nameNodeIP, nameNodeRegPort);
			nameNode = (NameNodeInterface) nameNodeRegistry.lookup(nameNodeService);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void registerMapperTask(int jobID, JobConfiguration jobConf,
			HashMap<Integer, String> chunkSets) throws IOException {
		System.out.println("This node need to handle the chunk number is: "
				+ chunkSets.size());
		int count = 0, mapNums = 0;
		Hashtable<Integer, ArrayList<KVPair>> mappers = new Hashtable<Integer, ArrayList<KVPair>>();
		ArrayList<KVPair> pairs = null;
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

		System.out.println("It needs " + mapNums + "mappers!");
		
		TaskStatusInfo taskStatusInfo;
		if(jobID_taskStatus.containsKey(jobID)) {
			taskStatusInfo = jobID_taskStatus.get(jobID);
		} else {
			taskStatusInfo = new TaskStatusInfo();	
		}
		taskStatusInfo.setTotalMapTasks(taskStatusInfo.getTotalMapTasks() + mapNums);
		taskStatusInfo.setUnfinishedMapTasks(taskStatusInfo.getUnfinishedMapTasks() + mapNums);
		jobID_taskStatus.put(jobID, taskStatusInfo);
		
		localizeMapTask(jobID);

		for (Integer mapperNum : mappers.keySet()) {
			ArrayList<KVPair> chunksAndNodes = mappers.get(mapperNum);
			String mapperName = jobTracker.getMapperInfo(jobID).getKey().toString();
			RMIServiceInfo rmiServiceInfo = new RMIServiceInfo();
			rmiServiceInfo.settingForMapper(dataNodeRegPort, dataNodeService,partitionNums,partitionFilePath);
			startMapTask(jobID, chunksAndNodes.size(), jobConf, chunksAndNodes,
					mapperName, mapperNum, rmiServiceInfo,1);
		}
	}
	
	public void localizeMapTask(int jobID) throws IOException {
		KVPair mapInfo = jobTracker.getMapperInfo(jobID);
		mapperClassName = mapInfo.getKey().toString().replace('.', '/') + ".class";
		byte[] mapperClassContent = (byte[]) mapInfo.getValue();
		IOUtil.writeBinary(mapperClassContent, mapperClassName);
	}

	public void startMapTask(int jobID, int numOfChunks, JobConfiguration jobConf, 
			ArrayList<KVPair> pairLists, String classname, Integer mapperNum, RMIServiceInfo rmiServiceInfo,int tryNums) {
		
		MapRunner mapRunner = new MapRunner(jobID, numOfChunks, jobConf, pairLists, classname, mapperNum, rmiServiceInfo,1);
		executor.execute(mapRunner);
	}

	public void registerReduceTask(int jobID, int partitionNo, HashMap<String, ArrayList<String>> nodesWithPartitions, 
			int numOfPartitions) throws IOException {
		// Update task status 
		TaskStatusInfo taskStatusInfo;
		if(jobID_taskStatus.containsKey(jobID)) {
			taskStatusInfo = jobID_taskStatus.get(jobID);
		} else {
			taskStatusInfo = new TaskStatusInfo();	
		}
		taskStatusInfo.setTotalReduceTasks(taskStatusInfo.getTotalReduceTasks() + numOfPartitions);
		taskStatusInfo.setUnfinishedReduceTasks(taskStatusInfo.getUnfinishedReduceTasks() + numOfPartitions);
		jobID_taskStatus.put(jobID, taskStatusInfo);
		
		localizeReduceTask(jobID);
		RMIServiceInfo rmiServiceInfo = new RMIServiceInfo();
		rmiServiceInfo.settingForReducer(taskTrackerRegPort, taskTrackServiceName);
		startReduceTask(jobID, partitionNo, nodesWithPartitions, reducerClassName, rmiServiceInfo);
	}

	public void localizeReduceTask(int jobID) throws IOException {
		KVPair reducerInfo = jobTracker.getReducerInfo(jobID);
		reducerClassName = reducerInfo.getKey().toString().replace('.', '/') + ".class";
		byte[] reducerClassContent = (byte[]) reducerInfo.getValue();
		IOUtil.writeBinary(reducerClassContent, reducerClassName);
	}

	public void startReduceTask(int jobID, int partitionNo, HashMap<String, ArrayList<String>> nodesWithPartitions, 
			String className, RMIServiceInfo rmiServiceInfo) {
		ReduceRunner reduceRunner = new ReduceRunner(jobID, partitionNo, nodesWithPartitions, className, rmiServiceInfo);
		executor.execute(reduceRunner);
	}
	
	@Override
	public byte[] getPartitionContent(String path) throws IOException {
		return IOUtil.readFile(path);
	}
	
	public static void updateMapStatus(Integer jobID, boolean isSuccessful) {
		if(isSuccessful) {
			TaskStatusInfo taskStatusInfo = TaskTracker.jobID_taskStatus.get(jobID);
			int curUnfinishedMapTasks = taskStatusInfo.getUnfinishedMapTasks() - 1;
			taskStatusInfo.setUnfinishedMapTasks(curUnfinishedMapTasks);
			TaskTracker.jobID_taskStatus.put(jobID, taskStatusInfo);
			
			if(curUnfinishedMapTasks == 0) {
				jobTracker.notifyMapperFinish(node, jobID_taskStatus, jobID_parFilePath);
			} 
		}
	}
	
	public static void updateReduceStatus(Integer jobID, boolean isSuccessful) {
		if(isSuccessful) {
			TaskStatusInfo taskStatusInfo = TaskTracker.jobID_taskStatus.get(jobID);
			int curUnfinishedReduceTasks = taskStatusInfo.getUnfinishedReduceTasks() - 1;
			taskStatusInfo.setUnfinishedReduceTasks(curUnfinishedReduceTasks);
			TaskTracker.jobID_taskStatus.put(jobID, taskStatusInfo);
			
			if(curUnfinishedReduceTasks == 0) {
				jobTracker.notifyReducerFinish(node, jobID_taskStatus);
			} 
		}
	}
	
	public static void updateFilePaths(Integer jobID, ArrayList<String> filePaths) {
		ArrayList<String> parFilePath = null;
		if(!jobID_parFilePath.containsKey(jobID)) {
			parFilePath = new ArrayList<String>();
		} else {
			parFilePath = jobID_parFilePath.get(jobID);
		}
		parFilePath.addAll(filePaths);
	}
	
	private void transmitHeartBeat() {
		System.out.println("Sending task progress to JobTracker...");
		TimerTask timerTask = new TimerTask() {

			@Override
			public void run() {
				jobTracker.responseToHeartbeat(node, jobID_taskStatus);
			}	
		};
		new Timer().scheduleAtFixedRate (timerTask, 0, 5000);
	}
	
	public static void handleDataNodeFailure (Integer jobID, Integer numOfChunks, JobConfiguration jobConf,
			ArrayList<KVPair> pairLists, String classname, Integer mapperNum, RMIServiceInfo rmiServiceInfo,int tryNums) {
		
		if(tryNums < jobMaxFailureThreshold) {
			ArrayList<Integer> chunks = new ArrayList<Integer>();
			ArrayList<KVPair> pairs = new ArrayList<KVPair>();
			
			for(KVPair kvPair : pairLists) {
				chunks.add((Integer)kvPair.getKey());
			}
			Hashtable<Integer,HashSet<String>> chunkDistribution = nameNode.getFileDistributionTable().get(jobConf.getInputfile());
			HashSet<String> healthyNodes = nameNode.getHealthyNodes();
			
			for(int i = 0; i < chunks.size(); i++) {
				if(healthyNodes.contains((String)pairLists.get(i).getValue())) {
					continue;
				}
				HashSet<String> distribution = chunkDistribution.get(chunks.get(i));
				for(String node : distribution) {
					if(healthyNodes.contains(node)) {
						pairs.add(new KVPair(chunks.get(i), node));
					}
					break;
				}
			}
			taskTracker.startMapTask(jobID, numOfChunks, jobConf, pairs, classname, mapperNum, rmiServiceInfo,tryNums++);
		} else {
			jobTracker.updateJobStatus(jobID, JobStatus.FAIL);
			jobTracker.terminateJob(jobID);
			System.out.println("Job terminated!");
			System.exit(-1);
		}
		
	}
	
	public static void handleMapperNodeFailure (int jobID) {
		jobTracker.terminateJob(jobID);
	}
	
	public void remove (int jobID) {
		jobID_parFilePath.remove(jobID);
		jobID_taskStatus.remove(jobID);
		jobID_node_mapID.remove(jobID);
	}
	
	

	public static void main(String args[]) throws IOException {
		try {
			taskTracker = new TaskTracker();
			IOUtil.readConf(mapredConf, taskTracker);
			IOUtil.readConf(dfsConf, taskTracker);

			TaskTrackerInterface stub = (TaskTrackerInterface) exportObject(taskTracker, taskPort);
			Registry registry = LocateRegistry.createRegistry(taskTrackerRegPort);
			registry.rebind(taskTrackServiceName, stub);
			
			node = InetAddress.getLocalHost().getHostAddress();
			System.out.println("I'm the TaskTracker for node " + node);
			// Continuously sending heart beat to job tracker.
			taskTracker.transmitHeartBeat();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
}
