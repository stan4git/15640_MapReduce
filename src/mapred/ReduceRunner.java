/**
 * 
 */
package mapred;

import java.io.UnsupportedEncodingException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import dfs.NameNodeInterface;
import format.KVPair;
import format.ReducerOutputCollector;
import format.OutputFormat;
import util.IOUtil;

/**
 * @author menglonghe
 * @author sidilin
 *
 */
public class ReduceRunner {
	
	private static Reducer reducer;
	private static NameNodeInterface nameNode;
	
	private static String nameNodeIP;
	private static Integer nameNodeRegPort;
	private static String nameNodeService;
	private static Integer taskTrackerRegPort;
	private static String taskTrackServiceName;
	
	private int jobID;
	private int partitionNo;
	private HashMap<Integer, HashMap<String, String>> job_nodesWithPartitions;
	private String className;
	
	public ReduceRunner (int jobID, int partitionNo, HashMap<Integer, HashMap<String, String>> job_nodesWithPartitions, String className) {
		 
		try {
			Registry reigstry = LocateRegistry.getRegistry(nameNodeIP, nameNodeRegPort);
			nameNode = (NameNodeInterface)reigstry.lookup(nameNodeService);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		
		this.jobID = jobID;
		this.partitionNo = partitionNo;
		this.job_nodesWithPartitions = job_nodesWithPartitions;
		this.className = className;
	}
	
	@SuppressWarnings("unchecked")
	public void start () {
		try {
			Class<Reducer> reduceClass = (Class<Reducer>) Class.forName(className);
			reducer = reduceClass.newInstance();
			HashSet<String> pathsForPartition = new HashSet<String>();
			HashMap<String, String> nodesWithPartitions = job_nodesWithPartitions.get(jobID);
			
			for (String node : nodesWithPartitions.keySet()) {
				try {
					Registry registry = LocateRegistry.getRegistry(node, taskTrackerRegPort);
					TaskTrackerInterface taskTracker = (TaskTrackerInterface) registry.lookup(taskTrackServiceName);
					for (String path : nodesWithPartitions.keySet()) {
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
			ReducerOutputCollector outputCollector = new ReducerOutputCollector();

			ArrayList<KVPair> formattedInput= Merger.combineValues(pathsForPartition);
			
			for(KVPair kv : formattedInput) {
				reducer.reduce((String)kv.getKey(), (ArrayList<String>)kv.getValue(), outputCollector);
			}
			
			OutputFormat outputFormat = new OutputFormat();
			String formattedOutput = outputFormat.formatOutput(outputCollector);
			byte[] outputForDFS = formattedOutput.getBytes("UTF-8");
			IOUtil.writeBinary(outputForDFS, "job-" + jobID + "-output-" + partitionNo);
						
			// Upload the output file into DFS !

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
	}

}
