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
	
	private String nameNodeIP;
	private Integer nameNodeRegPort;
	private String nameNodeService;
	private Integer taskTrackerRegPort;
	private String taskTrackServiceName;
	private String jobOutputPath;
	
	private int jobID;
	private int partitionNo;
	private HashMap<String, ArrayList<String>> nodesWithPartitions;
	private String className;
	
	
	public ReduceRunner (int jobID, int partitionNo, HashMap<String, ArrayList<String>> nodesWithPartitions, 
			String className, RMIServiceInfo rmiServiceInfo) {
		 
		this.jobID = jobID;
		this.partitionNo = partitionNo;
		this.nodesWithPartitions = nodesWithPartitions;
		this.className = className;
		
		this.nameNodeIP = rmiServiceInfo.getNameNodeIP();
		this.nameNodeRegPort = rmiServiceInfo.getNameNodeRegPort();
		this.nameNodeService = rmiServiceInfo.getNameNodeService();
		this.taskTrackerRegPort = rmiServiceInfo.getTaskTrackerRegPort();
		this.taskTrackServiceName = rmiServiceInfo.getTaskTrackServiceName();
		this.jobOutputPath = rmiServiceInfo.getJobOutputPath();
	}
	
	@SuppressWarnings("unchecked")
	public void start () {
		try {
			Class<Reducer> reduceClass = (Class<Reducer>) Class.forName(className);
			reducer = reduceClass.newInstance();
			HashSet<String> pathsForPartition = new HashSet<String>();
			
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
			
			try {
				Registry reigstry = LocateRegistry.getRegistry(nameNodeIP, nameNodeRegPort);
				nameNode = (NameNodeInterface)reigstry.lookup(nameNodeService);
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			}
			
			
			
			TaskTracker.updateMapStatus(jobID, true);

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
