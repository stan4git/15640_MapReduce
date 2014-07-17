/**
 * 
 */
package mapred;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import dfs.DFSClient;
import format.KVPair;
import format.ReducerOutputCollector;
import format.OutputFormat;
import util.IOUtil;

/**
 * This class is a thread to run the Reducer work
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public class ReduceRunner implements Runnable {
	
	private static Reducer reducer;
	// Task Tracker RMI info
	private Integer taskTrackerRegPort;
	private String taskTrackServiceName;
	
	private int jobID;
	// the partition number
	private int partitionNo;
	// nodes - > partitions
	private HashMap<String, ArrayList<String>> nodesWithPartitions;
	// the reduce class name
	private String className;
	// the written path for the reducing phrase
	private String reduceResultPath;
	// mapper result temporary path
	private String mapResTemporaryPath;
	// output file name 
	private String outputFileName;
	// DFSClient object
	private DFSClient dfsClient;
	
	/**
	 * The default constructor
	 * @param jobID
	 * @param partitionNo
	 * @param nodesWithPartitions
	 * @param className
	 * @param rmiServiceInfo
	 * @param reduceResultPath
	 */
	public ReduceRunner (int jobID, int partitionNo, HashMap<String, ArrayList<String>> nodesWithPartitions, 
			String className, RMIServiceInfo rmiServiceInfo, 
			String reduceResultPath, String mapResTemporaryPath,String outputFileName,DFSClient dfsClient) {
		 
		this.jobID = jobID;
		this.partitionNo = partitionNo;
		this.nodesWithPartitions = nodesWithPartitions;
		this.className = className;
		this.taskTrackerRegPort = rmiServiceInfo.getTaskTrackerRegPort();
		this.taskTrackServiceName = rmiServiceInfo.getTaskTrackServiceName();
		this.reduceResultPath = reduceResultPath;
		this.mapResTemporaryPath = mapResTemporaryPath;
		this.outputFileName = outputFileName;
		this.dfsClient = dfsClient;
	}
	
	/***
	 * This method is used to implement the reducer process.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void run () {
		Class<Reducer> reduceClass;
		try {
			//step1 : get the programmer's Reducer class and Instantiate it
			reduceClass = (Class<Reducer>) Class.forName(className);
			Constructor<Reducer> constructors = reduceClass.getConstructor();
			reducer = constructors.newInstance();
			HashSet<String> pathsForPartition = new HashSet<String>();
			
			//step2 : get files for specific partition from data nodes and localize them.
			for (String node : nodesWithPartitions.keySet()) {
				try {
					Registry registry = LocateRegistry.getRegistry(node, taskTrackerRegPort);
					TaskTrackerInterface taskTracker = (TaskTrackerInterface) registry.lookup(taskTrackServiceName);
					for (String path : nodesWithPartitions.get(node)) {
						String[] pathArray = path.split("/");
						String lastWord = pathArray[pathArray.length - 1];
						String effectivePath = "partition" + partitionNo;
						if(effectivePath.equals(lastWord)){
							byte[] content = taskTracker.getPartitionContent(path);
							String outputPath = mapResTemporaryPath + node + pathArray[3] + pathArray[4] + effectivePath;
							pathsForPartition.add(outputPath);
							IOUtil.writeBinary(content, outputPath);
						}
					}
				} catch (RemoteException e) {
					e.printStackTrace();
				} catch (NotBoundException e) {
					e.printStackTrace();
				} 
			}
			// step3 : sort the result and format it.
			ReducerOutputCollector outputCollector = new ReducerOutputCollector();
			ArrayList<KVPair> formattedInput= Merger.combineValues(pathsForPartition);
			
			for(KVPair kv : formattedInput) {
				reducer.reduce((String)kv.getKey(), (ArrayList<String>)kv.getValue(), outputCollector);
			}
			
			// step4 : localize the output.
			OutputFormat outputFormat = new OutputFormat();
			String formattedOutput = outputFormat.formatOutput(outputCollector);
			byte[] outputForDFS = formattedOutput.getBytes("UTF-8");
			String fileName = "job-" + jobID + "-" + outputFileName + "-" + partitionNo;
			String reduceFileName = reduceResultPath + fileName;
			IOUtil.writeBinary(outputForDFS, reduceFileName);
						
			// step 5 : upload the output file into DFS !
			dfsClient.putFile(reduceFileName);
			TaskTracker.setDFSClientAvailable();
			TaskTracker.updateReduceStatus(jobID, true);
		} catch (ClassNotFoundException | InstantiationException |
				IllegalAccessException | IOException | NoSuchMethodException | SecurityException
				| IllegalArgumentException | InvocationTargetException e) {
			TaskTracker.handleMapperNodeFailure(jobID);
			System.err.println("Reducer fails while fetching partitions !!");
			System.exit(-1);
		} 
	}
}
