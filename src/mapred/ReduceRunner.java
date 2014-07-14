/**
 * 
 */
package mapred;

import java.io.IOException;
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
 * @author menglonghe
 * @author sidilin
 *
 */
public class ReduceRunner implements Runnable {
	
	private static Reducer reducer;
	
	private Integer taskTrackerRegPort;
	private String taskTrackServiceName;
	
	private int jobID;
	private int partitionNo;
	private HashMap<String, ArrayList<String>> nodesWithPartitions;
	private String className;
	private String reduceResultPath;
	
	
	
	public ReduceRunner (int jobID, int partitionNo, HashMap<String, ArrayList<String>> nodesWithPartitions, 
			String className, RMIServiceInfo rmiServiceInfo, String reduceResultPath) {
		 
		this.jobID = jobID;
		this.partitionNo = partitionNo;
		this.nodesWithPartitions = nodesWithPartitions;
		this.className = className;
		this.taskTrackerRegPort = rmiServiceInfo.getTaskTrackerRegPort();
		this.taskTrackServiceName = rmiServiceInfo.getTaskTrackServiceName();
		this.reduceResultPath = reduceResultPath;
	}
	
	/***
	 * This method is used to implement the reducer process.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void run () {
		try {
			//step1 : get the programmer's Reducer class and Instantiate it
			Class<Reducer> reduceClass = (Class<Reducer>) Class.forName(className);
			reducer = reduceClass.newInstance();
			HashSet<String> pathsForPartition = new HashSet<String>();
			
			//step2 : get files for specific partition from data nodes and localize them.
			for (String node : nodesWithPartitions.keySet()) {
				try {
					Registry registry = LocateRegistry.getRegistry(node, taskTrackerRegPort);
					TaskTrackerInterface taskTracker = (TaskTrackerInterface) registry.lookup(taskTrackServiceName);
					for (String path : nodesWithPartitions.get(node)) {
						byte[] content = taskTracker.getPartitionContent(path);
						String outputPath = path;
						pathsForPartition.add(outputPath);
						IOUtil.writeBinary(content, outputPath);
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
			String fileName = "job-" + jobID + "-output-" + partitionNo;
			String reduceFileName = reduceResultPath + fileName;
			IOUtil.writeBinary(outputForDFS, reduceFileName);
						
			// step 5 : upload the output file into DFS !
			DFSClient dfsClient = new DFSClient();
			dfsClient.putFile(reduceFileName);
			TaskTracker.updateReduceStatus(jobID, true);
		} catch (ClassNotFoundException | InstantiationException |
				IllegalAccessException | IOException e) {
			TaskTracker.handleMapperNodeFailure(jobID);
			System.err.println("Reducer fails while fetching partitions !!");
			System.exit(-1);
		} 
	}
}
