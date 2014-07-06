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
import java.util.concurrent.ConcurrentHashMap;

import format.KVPair;
import util.IOUtil;

public class TaskTracker extends UnicastRemoteObject implements
		TaskTrackerInterface {

	private static final long serialVersionUID = -897603125687983899L;

	private static JobTrackerInterface jobTracker = null;

	private static final String mapredConf = "conf/mapred.conf";
	private static final String dfsConf = "conf/dfs.conf";

	private static String jobTrackerIP;
	private static Integer jobTrackerRegPort;
	private static String jobTrackServiceName;
	private static Integer taskPort;
	private static Integer taskTrackerRegPort;
	private static String taskTrackServiceName;
	private static Integer mapperChunkThreshold;

	public static ConcurrentHashMap<Integer, HashSet<String>> jobID_parFilePath = new ConcurrentHashMap<Integer, HashSet<String>>();
	public static ConcurrentHashMap<Integer, Integer> jobToUnfinishedMappers = new ConcurrentHashMap<Integer, Integer>();
	public static ConcurrentHashMap<Integer, Integer> jobTotalMappers = new ConcurrentHashMap<Integer, Integer>();
	public static Integer runningMappers = 0;

	private static String reducerClassName;
	private static String mapperClassName;
	private static Integer dataNodeRegPort;
	private static String dataNodeService;
	private static Integer partitionNums;
	private static String partitionFilePath;

	protected TaskTracker() throws RemoteException {
		super();

		try {
			Registry registry = LocateRegistry.getRegistry(jobTrackerIP,
					jobTrackerRegPort);
			jobTracker = (JobTrackerInterface) registry
					.lookup(jobTrackServiceName);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	public void registerReduceTask(int jobID, int partitionNo,
			HashSet<String> nodesWithPartitions) {
		localizeReduceTask(jobID);
		startReduceTask(jobID, partitionNo, nodesWithPartitions,
				reducerClassName);
	}

	public void localizeReduceTask(int jobID) {
		KVPair reducerInfo = jobTracker.getReducerInfo(jobID);
		reducerClassName = reducerInfo.getKey().toString().replace('.', '/')
				+ ".class";
		byte[] reducerClassContent = (byte[]) reducerInfo.getValue();
		IOUtil.writeBinary(reducerClassContent, reducerClassName);
	}

	public void localizeMapTask(int jobID) {
		KVPair mapInfo = jobTracker.getMapperInfo(jobID);
		mapperClassName = mapInfo.getKey().toString().replace('.', '/')
				+ ".class";
		byte[] mapperClassContent = (byte[]) mapInfo.getValue();
		IOUtil.writeBinary(mapperClassContent, mapperClassName);
	}

	public void startReduceTask(int jobID, int partitionNo,
			HashSet<String> nodesWithPartitions, String className) {
		ReduceRunner reduceRunner = new ReduceRunner(jobID, partitionNo,
				nodesWithPartitions, className);
		reduceRunner.start();
	}

	public String getPartitionContent(int jobID, int partitionNo) {
		HashSet<String> pathsForJob = jobID_parFilePath.get(jobID);
		HashSet<String> pathsForPartition = new HashSet<String>();
		for (String path : pathsForJob) {
			pathsForPartition.add(path + partitionNo);
		}
		return Merger.merge(pathsForPartition);
	}

	public static void main(String args[]) {

		try {
			TaskTracker taskTracker = new TaskTracker();
			IOUtil.readConf(mapredConf, taskTracker);
			IOUtil.readConf(dfsConf, taskTracker);

			TaskTrackerInterface stub = (TaskTrackerInterface) exportObject(
					taskTracker, taskPort);
			Registry registry = LocateRegistry
					.createRegistry(taskTrackerRegPort);
			registry.rebind(taskTrackServiceName, stub);

			System.out.println("I'm the TaskTracker for node "
					+ InetAddress.getLocalHost().getHostAddress());

		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
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
		if (count != mapperChunkThreshold) {
			mappers.put(mapNums, pairs);
			mapNums++;
		}

		System.out.println("It needs " + mapNums + "mappers!");
		jobToUnfinishedMappers.put(jobID, mapNums);
		jobTotalMappers.put(jobID, mapNums);
		synchronized (runningMappers) {
			runningMappers += mapNums;
		}
		localizeMapTask(jobID);

		for (Integer mapperNum : mappers.keySet()) {
			ArrayList<KVPair> chunksAndNodes = mappers.get(mapperNum);
			String mapperName = jobTracker.getMapperInfo(jobID).getKey()
					.toString();
			startMapTask(jobID, chunksAndNodes.size(), jobConf, chunksAndNodes,
					dataNodeRegPort, dataNodeService, mapperName,
					partitionFilePath, partitionNums);
		}
	}

	public void startMapTask(int jobID, int numOfChunks,
			JobConfiguration jobConf, ArrayList<KVPair> pairLists, int regPort,
			String serviceName, String classname, String partitionPath,
			int numPartitions) {
		MapRunner mapRunner = new MapRunner(jobID, numOfChunks, jobConf,
				pairLists, regPort, serviceName, classname, partitionPath,
				numPartitions);
		mapRunner.start();
	}
}
