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
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dfs.NameNodeInterface;
import util.IOUtil;
import util.JobStatus;
import util.NodeStatus;
import util.PathConfiguration;
import format.KVPair;

/**
 * This class has to be deployed on the master node. It coordinates all the
 * slave node, dispatch the tasks to the nodes, monitoring all the status of
 * nodes and jobs.
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public class JobTracker extends UnicastRemoteObject implements
		JobTrackerInterface {

	private static final long serialVersionUID = 9023603070698668607L;

	private static JobTracker jobTracker = null;
	private static JobScheduler jobScheduler = null;
	private static NameNodeInterface nameNode = null;

	/**
	 * These 3 contains JobTracker's registry IP,registry port, service port and
	 * service name
	 */
	private static Integer jobTrackerPort;
	private static Integer jobTrackerRegPort;
	private static String jobTrackServiceName;

	/** The maximum task numbers including mapper and reducer per node */
	private static Integer maxTaskPerNode;
	// the weight of local nodes and global nodes
	private static Double localWeight;
	private static Double globalWeight;

	/** These 3 contains NameNode's registry IP,registry port and service name */
	private static String nameNodeIP;
	private static Integer nameNodeRegPort;
	private static String nameNodeService;

	/** These 2 contains TaksTracker's registry port and service name */
	private static Integer taskTrackerRegPort;
	private static String taskTrackServiceName;

	/** The path for uploading the programmer's Mapper and Reducer */
	private static String jobUploadPath;

	/** The number of partitions */
	private static Integer partitionNums;

	/** Create a thread pool */
	private static ExecutorService executor = Executors.newCachedThreadPool();

	/** Global Job ID */
	private static volatile Integer globaljobID = 0;

	/**
	 * jobID -> All map tasks, jobID -> unfinished Map Tasks jobID -> All reduce
	 * Tasks, jobID -> unfinished reduce tasks
	 */
	public static ConcurrentHashMap<Integer, HashMap<String, TaskStatusInfo>> jobID_node_taskStatus = new ConcurrentHashMap<Integer, HashMap<String, TaskStatusInfo>>();

	/** node -> status */
	public static HashMap<String, Boolean> node_status;

	/** Each node contains total tasks */
	public static ConcurrentHashMap<String, Integer> node_totalTasks = new ConcurrentHashMap<String, Integer>();

	/** <jobID,<Do Job Node,<ChunkID,Chunk host Node>>> */
	public static ConcurrentHashMap<Integer, HashMap<String, HashMap<Integer, String>>> jobID_mapTasks = new ConcurrentHashMap<Integer, HashMap<String, HashMap<Integer, String>>>();

	/** <jobID,<nodes with partition files, paths>> */
	public static ConcurrentHashMap<Integer, HashMap<String, ArrayList<String>>> jobID_nodes_partitionsPath = new ConcurrentHashMap<Integer, HashMap<String, ArrayList<String>>>();

	/** jobID - > MapReduce Name and MapReduce Path */
	public static ConcurrentHashMap<Integer, KVPair> jobID_mapRedName = new ConcurrentHashMap<Integer, KVPair>();
	public static ConcurrentHashMap<Integer, KVPair> jobID_mapRedPath = new ConcurrentHashMap<Integer, KVPair>();
	/** job's status */
	public static ConcurrentHashMap<Integer, JobStatus> jobID_status = new ConcurrentHashMap<Integer, JobStatus>();
	/** jobId - > JobConfiguration Instance */
	public static ConcurrentHashMap<Integer, JobConfiguration> jobID_configuration = new ConcurrentHashMap<Integer, JobConfiguration>();
	/** jobId - > map Failure times */
	public static ConcurrentHashMap<Integer, Integer> jobID_mapFailureTimes = new ConcurrentHashMap<Integer, Integer>();
	/** jobId - > reduce Failure times */
	public static ConcurrentHashMap<Integer, Integer> jobID_reduceFailureTimes = new ConcurrentHashMap<Integer, Integer>();
	/** The maximum failure tiems for each job */
	public static Integer jobMaxFailureThreshold;
	/** This table is used to record which JobID has started the reduce work */
	public static ConcurrentHashMap<Integer, Boolean> reduceWorkBeginning = new ConcurrentHashMap<Integer, Boolean>();
	/** <node, <jobID, chunkIDs>> */
	public static ConcurrentHashMap<String, HashMap<Integer, HashSet<Integer>>> node_jobID_chunkIDs = new ConcurrentHashMap<String, HashMap<Integer, HashSet<Integer>>>();
	/** <node,<jobID,partitionNo>> */

	public static ConcurrentHashMap<String,HashMap<Integer,HashSet<Integer>>> node_jobID_partitionNos = new ConcurrentHashMap<String,HashMap<Integer,HashSet<Integer>>>();
	/** <node, <jobID, nextMapID>> */
	public static ConcurrentHashMap<String,HashMap<Integer,Integer>> node_jobID_nextMapID = new ConcurrentHashMap<String,HashMap<Integer,Integer>>();

	protected JobTracker() throws RemoteException {
		super();
		globaljobID = 0;
	}

	@Override
	public String submitJob(JobConfiguration jobConf, KVPair mapper,
			KVPair reducer) throws IOException {

		// step1 : find if the input file exists on the DFS system.
		try {
			Registry reigstry = LocateRegistry.getRegistry(nameNodeIP,
					nameNodeRegPort);
			nameNode = (NameNodeInterface) reigstry.lookup(nameNodeService);
			if (!nameNode.fileExist(jobConf.getInputfile())) {
				return "INPUTNOTFOUND";
			}
		} catch (RemoteException e) {
//			e.printStackTrace();
		} catch (NotBoundException e) {
//			e.printStackTrace();
			return "FAIL";
		}
		// step 2: update the globaljobID
		Integer jobID = globaljobID;
		updateGlobaljobID();
		jobID_status.put(jobID, JobStatus.INPROGRESS);
		jobID_mapFailureTimes.put(jobID, 1);
		jobID_reduceFailureTimes.put(jobID, 1);

		// step 3: Get the working node and chunks from jobScheduler
		Hashtable<Integer, HashSet<String>> chunkDistribution = nameNode
				.getFileDistributionTable().get(jobConf.getInputfile());
		HashMap<String, HashMap<Integer, String>> nodeToChunks = jobScheduler
				.selectBestNodeToChunks(chunkDistribution);

		// step 4: copy the programmer's Mapper and Reducer into local directory
		if (nodeToChunks == null) {
			return "FAIL";
		} else {
			localizeJob(mapper, reducer, jobID);
		}

		// step 5: initial maps with jobID
		jobID_node_taskStatus.put(jobID, new HashMap<String, TaskStatusInfo>());
		jobID_nodes_partitionsPath.put(jobID,
				new HashMap<String, ArrayList<String>>());
		jobID_configuration.put(jobID, jobConf);
		reduceWorkBeginning.put(jobID, false);

		// step 6: Send work to node
		for (String node : nodeToChunks.keySet()) {
			if (jobID_node_taskStatus.get(jobID).get(node) == null) {
				HashMap<String, TaskStatusInfo> taskNodeStatus = jobID_node_taskStatus
						.get(jobID);
				taskNodeStatus.put(node, new TaskStatusInfo());
				jobID_node_taskStatus.put(jobID, taskNodeStatus);
			}

			if (jobID_nodes_partitionsPath.get(jobID).get(node) == null) {
				HashMap<String, ArrayList<String>> nodes_partitionsPath = jobID_nodes_partitionsPath
						.get(jobID);
				nodes_partitionsPath.put(node, new ArrayList<String>());
				jobID_nodes_partitionsPath.put(jobID, nodes_partitionsPath);
			}

			// update the <node, <jobID, chunkIDs>> relationship
			HashMap<Integer, HashSet<Integer>> jobID_chunkIDs = new HashMap<Integer, HashSet<Integer>>();
			jobID_chunkIDs.put(jobID, new HashSet<Integer>());
			for (int chunkID : nodeToChunks.get(node).keySet()) {
				jobID_chunkIDs.get(jobID).add(chunkID);
			}
			node_jobID_chunkIDs.put(node, jobID_chunkIDs);

			
			HashMap<Integer, Integer> jobID_nextMapID = new HashMap<Integer, Integer>();
			jobID_nextMapID.put(jobID, 0);
			node_jobID_nextMapID.put(node, jobID_nextMapID);
			
			System.out.println("choose node: " + node + " to run one or more Mapper tasks!");
		}

		for (String node : nodeToChunks.keySet()) {
			TaskThread mapTask = new TaskThread(node, jobID, jobConf,
					nodeToChunks.get(node), true, 0, null, 0,
					taskTrackerRegPort, taskTrackServiceName);
			executor.execute(mapTask);
		}
		return jobID.toString();
	}

	/**
	 * This method is used to handle the situation if some task node turns down,
	 * the system need to recover all the mappers and reducers on the down node.
	 * 
	 * @param node
	 * @throws RemoteException
	 */
	public synchronized void handleNodeFailure(String node)
			throws RemoteException {

		// step1 : set node as dead node
		nameNode.setNodeStatus(node, NodeStatus.DEAD);

		if (node_totalTasks.get(node) != null) {
			node_totalTasks.put(node, 0);
		}

		// step2 : handle failed mappers
		for (int jobID : node_jobID_chunkIDs.get(node).keySet()) {
			if (jobID_node_taskStatus.get(jobID).get(node)
					.getUnfinishedMapTasks() == 0) {
				continue;
			}
			HashSet<Integer> chunkIDs = node_jobID_chunkIDs.get(node)
					.get(jobID);
			handleMapperFailure(jobID, node, chunkIDs);
		}
		node_jobID_chunkIDs.put(node, new HashMap<Integer, HashSet<Integer>>());

		// step3 : handle failed reducers
		for (int jobID : node_jobID_partitionNos.get(node).keySet()) {
			if (jobID_node_taskStatus.get(jobID).get(node)
					.getUnfinishedReduceTasks() == 0) {
				continue;
			}
			for (int partitionNo : node_jobID_partitionNos.get(node).get(jobID)) {
				handleReducerFailure(jobID, partitionNo);
			}
		}
		node_jobID_partitionNos.put(node,
				new HashMap<Integer, HashSet<Integer>>());
	}

	/**
	 * set the node's status to unhealthy redistribute the chunks on this node
	 * update the distribution table start a new TaskThread.
	 * 
	 * @param jobID
	 * @param node
	 * @param chunks
	 * @throws RemoteException
	 */
	public synchronized static void handleMapperFailure(int jobID, String node,
			Set<Integer> chunks) throws RemoteException {

		int failureTimes = jobID_mapFailureTimes.get(jobID);

		if (failureTimes < jobMaxFailureThreshold) {
			Hashtable<Integer, HashSet<String>> chunkDistribution = nameNode
					.getFileDistributionTable().get(
							jobID_configuration.get(jobID).getInputfile());
			Hashtable<Integer, HashSet<String>> result = new Hashtable<Integer, HashSet<String>>();
			for (int chunk : chunks) {
				HashSet<String> nodes = chunkDistribution.get(chunk);
				result.put(chunk, nodes);
			}
			HashMap<String, HashMap<Integer, String>> nodeToChunks = jobScheduler
					.selectBestNodeToChunks(result);

			if (nodeToChunks == null) {
				System.err
						.println("The system has no extra replica for this chunk! This job will be terminated!");
				return;
			}

			node_jobID_chunkIDs.put(node,
					new HashMap<Integer, HashSet<Integer>>());
			//jobID_node_taskStatus.get(jobID).put(node, new TaskStatusInfo());
			jobID_node_taskStatus.get(jobID).remove(node);
			jobID_nodes_partitionsPath.get(jobID).put(node,
					new ArrayList<String>());

			for (String assignedNode : nodeToChunks.keySet()) {

				if (jobID_node_taskStatus.get(jobID).get(assignedNode) == null) {
					HashMap<String, TaskStatusInfo> taskNodeStatus = jobID_node_taskStatus
							.get(jobID);
					taskNodeStatus.put(assignedNode, new TaskStatusInfo());
					jobID_node_taskStatus.put(jobID, taskNodeStatus);
				}

				if (jobID_nodes_partitionsPath.get(jobID).get(assignedNode) == null) {
					HashMap<String, ArrayList<String>> nodes_partitionsPath = jobID_nodes_partitionsPath
							.get(jobID);
					nodes_partitionsPath.put(assignedNode,
							new ArrayList<String>());
					jobID_nodes_partitionsPath.put(jobID, nodes_partitionsPath);
				}

				// update the <node, <jobID, chunkIDs>> relationship
				if (node_jobID_chunkIDs.get(assignedNode) == null) {
					node_jobID_chunkIDs.put(assignedNode,
							new HashMap<Integer, HashSet<Integer>>());
				}

				HashSet<Integer> chunkIDs = new HashSet<Integer>();
				for (int chunkID : nodeToChunks.get(assignedNode).keySet()) {
					chunkIDs.add(chunkID);
				}

				node_jobID_chunkIDs.get(assignedNode).put(jobID, chunkIDs);

				System.out.println("choose node: " + assignedNode
						+ " to run one or more Mapper tasks!");
			}

			for (String assignedNode : nodeToChunks.keySet()) {
				TaskThread mapTask = new TaskThread(assignedNode, jobID,
						jobID_configuration.get(jobID),
						nodeToChunks.get(assignedNode), true, 0, null, 0,
						taskTrackerRegPort, taskTrackServiceName);
				executor.execute(mapTask);
			}

			jobID_mapFailureTimes.put(jobID, failureTimes + 1);

		} else {
			jobID_status.put(jobID, JobStatus.FAIL);
			jobTracker.terminateJob(jobID);
			System.out.println("Job terminated!");
		}
	}

	@Override
	public String getOutputFileName(int jobID) {
		return jobID_configuration.get(jobID).getOutputfile();
	}

	/**
	 * set the node's status to unhealthy redistribute the chunks on this node
	 * update the distribution table start a new TaskThread
	 * 
	 * @param jobID
	 * @param partitionNo
	 * @throws RemoteException
	 */
	public synchronized static void handleReducerFailure(int jobID,
			int partitionNo) throws RemoteException {

		int failureTimes = jobID_mapFailureTimes.get(jobID);
		if (failureTimes < jobMaxFailureThreshold) {
			ArrayList<String> chosenReduceNodes = jobScheduler
					.pickBestNodesForReduce(1);
			HashMap<String, ArrayList<String>> nodes_partitionsPath = jobID_nodes_partitionsPath
					.get(jobID);
			if (chosenReduceNodes == null) {
				System.out.println("System is busy, the job fails");
				jobID_status.put(jobID, JobStatus.FAIL);
				return;
			}

			// update node_jobID_partitionNos start
			String node = chosenReduceNodes.get(0);
			if (node_jobID_partitionNos.get(node) == null) {
				node_jobID_partitionNos.put(node,
						new HashMap<Integer, HashSet<Integer>>());
			}
			HashMap<Integer, HashSet<Integer>> jobID_partitionNos = node_jobID_partitionNos
					.get(node);
			HashSet<Integer> partitionNos = null;
			if (jobID_partitionNos.get(jobID) == null) {
				partitionNos = new HashSet<Integer>();
			} else {
				partitionNos = jobID_partitionNos.get(jobID);
			}
			partitionNos.add(partitionNo);
			jobID_partitionNos.put(jobID, partitionNos);
			node_jobID_partitionNos.put(node, jobID_partitionNos);

			TaskThread reduceTask = new TaskThread(node, jobID, null, null,
					false, partitionNo, nodes_partitionsPath, partitionNums,
					taskTrackerRegPort, taskTrackServiceName);
			executor.execute(reduceTask);
			jobID_mapFailureTimes.put(jobID, failureTimes + 1);
		} else {
			jobID_status.put(jobID, JobStatus.FAIL);
			jobTracker.terminateJob(jobID);
			System.out.println("Job terminated!");
		}
	}

	/**
	 * This method is used to increase the global Job ID
	 */
	public void updateGlobaljobID() {
		synchronized (globaljobID) {
			globaljobID++;
		}
	}

	@Override
	public void localizeJob(KVPair mapper, KVPair reducer, Integer jobID)
			throws IOException {

		// KVPair mapper
		// key: wordCount.wordMapper
		// value: wordCount/wordMapper.class
		String[] mappers = ((String) mapper.getKey()).split("\\.");
		String[] reducers = ((String) reducer.getKey()).split("\\.");
		// mapperPath:/tmp/upload/wordMapper-0.class
		String mapperPath = jobUploadPath + mappers[mappers.length - 1] + "-"
				+ jobID + ".class";
		String reducerPath = jobUploadPath + reducers[reducers.length - 1]
				+ "-" + jobID + ".class";

		IOUtil.writeBinary(mapper.getValue().toString().getBytes(), mapperPath);
		IOUtil.writeBinary(reducer.getValue().toString().getBytes(),
				reducerPath);

		KVPair mapRedName = new KVPair((String) mapper.getKey(),
				(String) reducer.getKey());
		KVPair mapRedPath = new KVPair(mapperPath, reducerPath);

		jobID_mapRedName.put(jobID, mapRedName);
		jobID_mapRedPath.put(jobID, mapRedPath);
	}

	@Override
	public void startReducePhase(int jobID) throws RemoteException {
		// System.out.println("Start jobID's "+ jobID +" reduce job !!");
		int numOfPartitions = partitionNums;
		ArrayList<String> chosenReduceNodes = jobScheduler
				.pickBestNodesForReduce(numOfPartitions);
		HashMap<String, ArrayList<String>> nodes_partitionsPath = jobID_nodes_partitionsPath
				.get(jobID);
		if (chosenReduceNodes == null) {
			System.out.println("System is busy, the job fails");
			jobID_status.put(jobID, JobStatus.FAIL);
			return;
		}

		// update node_jobID_partitionNos beginning
		HashMap<String, HashSet<Integer>> node_partitionNos = new HashMap<String, HashSet<Integer>>();
		HashSet<Integer> partitions = null;
		for (int i = 0; i < chosenReduceNodes.size(); i++) {
			String node = chosenReduceNodes.get(i);
			if (node_partitionNos.containsKey(node)) {
				partitions = node_partitionNos.get(node);
			} else {
				partitions = new HashSet<Integer>();
			}
			partitions.add(i);
			node_partitionNos.put(node, partitions);
		}

		for (String node : node_partitionNos.keySet()) {
			if (node_jobID_partitionNos.get(node) == null) {
				node_jobID_partitionNos.put(node,
						new HashMap<Integer, HashSet<Integer>>());
			}
			node_jobID_partitionNos.get(node).put(jobID,
					node_partitionNos.get(node));
		}
		// update node_jobID_partitionNos end

		for (int i = 0; i < numOfPartitions; i++) {
			TaskThread reduceTask = new TaskThread(chosenReduceNodes.get(i),
					jobID, null, null, false, i, nodes_partitionsPath, 1,
					taskTrackerRegPort, taskTrackServiceName);
			executor.execute(reduceTask);
		}
	}

	@Override
	public KVPair getReducerInfo(int jobID) throws IOException {
		String reducerClassName = (String) jobID_mapRedName.get(jobID)
				.getValue();
		String reducerClassPath = (String) jobID_mapRedPath.get(jobID)
				.getValue();
		return new KVPair(reducerClassName, IOUtil.readFile(reducerClassPath));
	}

	@Override
	public KVPair getMapperInfo(int jobID) throws IOException {
		String mapperClassName = (String) jobID_mapRedName.get(jobID).getKey();
		String mapperClassPath = (String) jobID_mapRedPath.get(jobID).getKey();
		return new KVPair(mapperClassName, IOUtil.readFile(mapperClassPath));
	}

	@Override
	public void terminateJob(int jobID) {

		// if(!jobID_node_taskStatus.containsKey(jobID)) {
		// return;
		// }
		// //
		// // for (String node : jobID_node_taskStatus.get(jobID).keySet()) {
		// // try {
		// // Registry registry = LocateRegistry.getRegistry(node,
		// taskTrackerRegPort);
		// // TaskTrackerInterface taskTracker = (TaskTrackerInterface)
		// registry.lookup(taskTrackServiceName);
		// // taskTracker.remove(jobID);
		// // } catch (RemoteException e) {
		// // e.printStackTrace();
		// // } catch (NotBoundException e) {
		// // e.printStackTrace();
		// // }
		// // }
		//
		// jobID_node_taskStatus.remove(jobID);
		// jobID_mapTasks.remove(jobID);
		// jobID_mapRedName.remove(jobID);
		// jobID_mapRedPath.remove(jobID);
		// jobID_mapFailureTimes.remove(jobID);
		// jobID_reduceFailureTimes.remove(jobID);
		// jobID_configuration.remove(jobID);
		// jobID_status.remove(jobID);
	}

	@Override
	public JobStatus checkJobStatus(Integer jobID) {
		return jobID_status.get(jobID);
	}

	@Override
	public double getMapperProgress(Integer jobID) {
		int totalMapTasks = 0;
		int unfinishedMapTasks = 0;

		for (String node : jobID_node_taskStatus.get(jobID).keySet()) {
			totalMapTasks += jobID_node_taskStatus.get(jobID).get(node)
					.getTotalMapTasks();
			unfinishedMapTasks += jobID_node_taskStatus.get(jobID).get(node)
					.getUnfinishedMapTasks();
		}

		if (totalMapTasks == 0) {
			return 0;
		}
		return 1 - (double) unfinishedMapTasks / (double) totalMapTasks;
	}

	@Override
	public double getReducerProgress(Integer jobID) {
		int totalReduceTasks = 0;
		int unfinishedReduceTasks = 0;

		for (String node : jobID_node_taskStatus.get(jobID).keySet()) {
			totalReduceTasks += jobID_node_taskStatus.get(jobID).get(node)
					.getTotalReduceTasks();
			totalReduceTasks += jobID_node_taskStatus.get(jobID).get(node)
					.getUnfinishedReduceTasks();
		}

		if (totalReduceTasks == 0) {
			return 0;
		}

		return 1 - (double) unfinishedReduceTasks / (double) totalReduceTasks;
	}

	@Override
	public synchronized void notifyMapperFinish(String node,
			ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus,
			ConcurrentHashMap<Integer, ArrayList<String>> jobID_parFilePath,
			int jobID) throws RemoteException {

		int unfinishedMapTasks = 0;
		int unfinishedReduceTasks = 0;

		// update jobID_nodes_partitionsPath
		HashMap<String, ArrayList<String>> nodes_Paths = jobID_nodes_partitionsPath.get(jobID);
		nodes_Paths.get(node).addAll(jobID_parFilePath.get(jobID));
		jobID_nodes_partitionsPath.put(jobID, nodes_Paths);

		for (int jobId : jobID_taskStatus.keySet()) {
			// step1: update jobID_node_taskStatus
			if (jobID_status.get(jobId) == null) {
				continue;
			}
			TaskStatusInfo taskStatusInfo = jobID_taskStatus.get(jobId);
			jobID_node_taskStatus.get(jobId).put(node, taskStatusInfo);
			unfinishedMapTasks += taskStatusInfo.getUnfinishedMapTasks();
			unfinishedReduceTasks += taskStatusInfo.getUnfinishedReduceTasks();

			// System.err.println(reduceWorkBeginning.toString());

			// step2: if the whole mapper process has finished, start reduce
			// phase.
			if (isMapperJobFinished(jobId) && !reduceWorkBeginning.get(jobId)) {
				reduceWorkBeginning.put(jobId, true);
				startReducePhase(jobId);
			}
		}

		node_totalTasks.put(node, unfinishedMapTasks + unfinishedReduceTasks);

	}

	@Override
	public synchronized void notifyReducerFinish(String node,
			ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus) {
		int unfinishedMapTasks = 0;
		int unfinishedReduceTasks = 0;

		for (int jobID : jobID_taskStatus.keySet()) {
			// step1: update jobID_node_taskStatus
			TaskStatusInfo taskStatusInfo = jobID_taskStatus.get(jobID);
			jobID_node_taskStatus.get(jobID).put(node, taskStatusInfo);
			unfinishedMapTasks += taskStatusInfo.getUnfinishedMapTasks();
			unfinishedReduceTasks += taskStatusInfo.getUnfinishedReduceTasks();

			if (jobID_status.get(jobID) != null
					&& jobID_status.get(jobID).equals(JobStatus.SUCCESS)) {
				continue;
			}
			if (isReducerJobFinished(jobID)) {
				jobID_status.put(jobID, JobStatus.SUCCESS);
				System.out.println("The Job which ID: " + jobID
						+ " has been excuted successfully!");
				// terminateJob(jobID);
			}
		}

		node_totalTasks.put(node, unfinishedMapTasks + unfinishedReduceTasks);
	}

	/**
	 * This method is used to transmit the HeartBeat
	 */
	private void transmitHeartBeat() {
		// System.out.println("Sending task progress to JobTracker...");
		Registry reigstry;
		ConcurrentHashMap<String, TaskTrackerInterface> node_taskTrackers = new ConcurrentHashMap<String, TaskTrackerInterface>();

		TaskTrackerInterface taskTracker = null;
		ConcurrentHashMap<Integer, TaskStatusInfo> jobID_taskStatus = null;

		
		for (String node : node_totalTasks.keySet()) {
//			int retryThreshold = 3;
//			boolean success = false;
//			while (!success && retryThreshold > 0) {
				try {
					if (node_taskTrackers.containsKey(node)) {
						taskTracker = node_taskTrackers.get(node);
					} else {
						reigstry = LocateRegistry.getRegistry(node,
								taskTrackerRegPort);
						taskTracker = (TaskTrackerInterface) reigstry
								.lookup(taskTrackServiceName);
						node_taskTrackers.put(node, taskTracker);
					}
					jobID_taskStatus = taskTracker.heartBeat();

					int unfinishedMapTasks = 0;
					int unfinishedReduceTasks = 0;

					for (int jobID : jobID_taskStatus.keySet()) {
						TaskStatusInfo taskStatusInfo = jobID_taskStatus
								.get(jobID);
						jobID_node_taskStatus.get(jobID).put(node,
								taskStatusInfo);
						unfinishedMapTasks += taskStatusInfo
								.getUnfinishedMapTasks();
						unfinishedReduceTasks += taskStatusInfo
								.getUnfinishedReduceTasks();
					}

					node_totalTasks.put(node, unfinishedMapTasks
							+ unfinishedReduceTasks);
//					success = true;
				} catch (RemoteException | NotBoundException e) {
//					retryThreshold--;
//					if (retryThreshold <= 0) {
						node_totalTasks.put(node, 0);
//						e.printStackTrace();
//						break;
//					} else {
//						continue;
//					}
//				}
			}
		}
	}

	/**
	 * This method is a heartBeat timer
	 */
	public void heartBeatTimer() {
		TimerTask timerTask = new TimerTask() {

			@Override
			public void run() {
				transmitHeartBeat();
			}
		};
		new Timer().scheduleAtFixedRate(timerTask, 0, 5000);
	}

	/**
	 * This method is used to judge whether the Mapper has finished on specific
	 * job ID
	 * 
	 * @param jobID
	 * @return boolean judge whether the map task is finished
	 */
	public boolean isMapperJobFinished(int jobID) {
		HashMap<String, TaskStatusInfo> node_status = jobID_node_taskStatus.get(jobID);
		for (String nodeIP : node_status.keySet()) {
			TaskStatusInfo taskStatusInfo = node_status.get(nodeIP);
			if (taskStatusInfo.getTotalMapTasks() == 0
					|| taskStatusInfo.getUnfinishedMapTasks() != 0) {
				return false;
			}
		}
		return true;
	}

	/**
	 * This method is used to judge whether the Reducer has finished on specific
	 * job ID
	 * 
	 * @param jobID
	 * @return boolean judge whether the reduce task is finished
	 */
	public boolean isReducerJobFinished(int jobID) {
		HashMap<String, TaskStatusInfo> node_status = jobID_node_taskStatus
				.get(jobID);
		for (String nodeIP : node_status.keySet()) {
			TaskStatusInfo taskStatusInfo = node_status.get(nodeIP);
			if (taskStatusInfo.getUnfinishedReduceTasks() != 0) {
				return false;
			}
		}
		return true;
	}

	@Override
	public synchronized void updateJobStatus(Integer jobId, JobStatus jobStatus) {
		jobID_status.put(jobId, jobStatus);
	}

	public static void main(String args[]) throws IOException {
		try {
			jobTracker = new JobTracker();

			// 1. Read DFS and MapReduce's configuration files and fill the
			// properties to the jobTrack object
			IOUtil.readConf(PathConfiguration.DFSConfPath, jobTracker);
			IOUtil.readConf(PathConfiguration.MapReducePath, jobTracker);

			// 2. Initialize the JobScheduler
			jobScheduler = new JobScheduler(nameNodeIP, nameNodeRegPort,
					nameNodeService, maxTaskPerNode, localWeight, globalWeight);

			// 3. Build the RMI Registry Server and bind the service to the
			// registry server
			unexportObject(jobTracker, false);
			JobTrackerInterface stub = (JobTrackerInterface) exportObject(
					jobTracker, jobTrackerPort);
			Registry registry = LocateRegistry
					.createRegistry(jobTrackerRegPort);
			registry.rebind(jobTrackServiceName, stub);
			InetAddress address = InetAddress.getLocalHost();
			System.out.println("The JobTracker's IP address is "
					+ address.getHostAddress());
			System.out.println("The JobTracker has started successfully!");

			// 4. Monitoring
			jobTracker.heartBeatTimer();
		} catch (RemoteException e) {
//			e.printStackTrace();
		} catch (UnknownHostException e) {
//			e.printStackTrace();
		}
	}

	@Override
	public synchronized void registerTaskTracker(String taskTrackerIP)
			throws RemoteException {
		node_totalTasks.put(taskTrackerIP, 0);
	}

	public synchronized static void invokeFailureHandleMethod(String node) {
		try {
			jobTracker.handleNodeFailure(node);
		} catch (RemoteException e) {
//			e.printStackTrace();
		}
	}
	
	@Override
	public synchronized void updateNextMapID (String node, int jobID, int value) throws RemoteException {
		HashMap<Integer, Integer> jobID_nextMapID = new HashMap<Integer, Integer>();
		jobID_nextMapID.put(jobID, value);
		node_jobID_nextMapID.put(node, jobID_nextMapID);
	}
	
	@Override
	public synchronized int getNextMapID (String node, int jobID) throws RemoteException  {
		return node_jobID_nextMapID.get(node).get(jobID);
	}

}
