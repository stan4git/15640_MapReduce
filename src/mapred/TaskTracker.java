package mapred;

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

import format.KVPair;
import util.IOUtil;

public class TaskTracker extends UnicastRemoteObject implements
		TaskTrackerInterface {

	private static final long serialVersionUID = -897603125687983899L;

	private static JobTrackerInterface jobTracker = null;
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
	private static String jobOutputPath;
	private static String jobTrackerIP;
	private static Integer jobTrackerRegPort;
	private static String jobTrackServiceName;
	private static Integer taskPort;
	private static Integer taskTrackerRegPort;
	private static String taskTrackServiceName;
	private static Integer mapperChunkThreshold;
	
	private static String node;

	protected TaskTracker() throws RemoteException {
		super();

		try {
			Registry registry = LocateRegistry.getRegistry(jobTrackerIP, jobTrackerRegPort);
			jobTracker = (JobTrackerInterface) registry.lookup(jobTrackServiceName);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void registerMapperTask(int jobID, JobConfiguration jobConf,
			HashMap<Integer, String> chunkSets) {
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
					mapperName, mapperNum, rmiServiceInfo);
		}
	}
	
	public void localizeMapTask(int jobID) {
		KVPair mapInfo = jobTracker.getMapperInfo(jobID);
		mapperClassName = mapInfo.getKey().toString().replace('.', '/') + ".class";
		byte[] mapperClassContent = (byte[]) mapInfo.getValue();
		IOUtil.writeBinary(mapperClassContent, mapperClassName);
	}

	public void startMapTask(int jobID, int numOfChunks, JobConfiguration jobConf, 
			ArrayList<KVPair> pairLists, String classname, Integer mapperNum, RMIServiceInfo rmiServiceInfo) {
		
		MapRunner mapRunner = new MapRunner(jobID, numOfChunks, jobConf, pairLists, classname, mapperNum, rmiServiceInfo);
		executor.execute(mapRunner);
		
		
	}

	public void registerReduceTask(int jobID, int partitionNo, HashMap<String, ArrayList<String>> nodesWithPartitions, 
			int numOfPartitions) {
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
		rmiServiceInfo.settingForReducer(nameNodeIP, nameNodeRegPort, nameNodeService, taskTrackerRegPort,
				taskTrackServiceName, jobOutputPath);
		startReduceTask(jobID, partitionNo, nodesWithPartitions, reducerClassName, rmiServiceInfo);
	}

	public void localizeReduceTask(int jobID) {
		KVPair reducerInfo = jobTracker.getReducerInfo(jobID);
		reducerClassName = reducerInfo.getKey().toString().replace('.', '/') + ".class";
		byte[] reducerClassContent = (byte[]) reducerInfo.getValue();
		IOUtil.writeBinary(reducerClassContent, reducerClassName);
	}

	public void startReduceTask(int jobID, int partitionNo, HashMap<String, ArrayList<String>> nodesWithPartitions, 
			String className, RMIServiceInfo rmiServiceInfo) {
		ReduceRunner reduceRunner = new ReduceRunner(jobID, partitionNo, nodesWithPartitions, className, rmiServiceInfo);
		reduceRunner.start();
	}
	
	@Override
	public byte[] getPartitionContent(String path) {
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
		} else {
			
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
		} else {
			
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
	
	

	public static void main(String args[]) {
		try {
			TaskTracker taskTracker = new TaskTracker();
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
