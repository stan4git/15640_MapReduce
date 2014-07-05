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
import java.util.HashSet;
import java.util.Iterator;

import dfs.NameNodeInterface;
import format.KVPair;
import format.OutputCollector;
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
	private HashSet<String> nodesWithPartitions;
	private String className;
	
	public ReduceRunner (int jobID, int partitionNo, HashSet<String> nodesWithPartitions, String className) {
		 
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
		this.nodesWithPartitions = nodesWithPartitions;
		this.className = className;
	}
	
	@SuppressWarnings("unchecked")
	public void start () {
		try {
			Class<Reducer> reduceClass = (Class<Reducer>) Class.forName(className);
			reducer = reduceClass.newInstance();
			HashSet<String> pathsForPartition = new HashSet<String>();
			
			Iterator<String> iterator = nodesWithPartitions.iterator();
			while(iterator.hasNext()) {
				String node = iterator.next();
				try {
					Registry registry = LocateRegistry.getRegistry(node, taskTrackerRegPort);
					TaskTrackerInterface taskTracker = (TaskTrackerInterface) registry.lookup(taskTrackServiceName);
					String sortedContent = taskTracker.getPartitionContent(jobID, partitionNo);
					String outputPath = jobID + "-mapper-partition-" + partitionNo;
					pathsForPartition.add(outputPath);
					IOUtil.writeBinary(sortedContent.getBytes("UTF-8"), outputPath);
					
				} catch (RemoteException e) {
					e.printStackTrace();
				} catch (NotBoundException e) {
					e.printStackTrace();
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
			OutputCollector outputCollector = new OutputCollector();
			ArrayList<KVPair> formattedInput= Merger.combineValues(pathsForPartition);
			
			for(KVPair kv : formattedInput) {
				reducer.reduce((String)kv.getKey(), (ArrayList<String>)kv.getValue(), outputCollector);
			}
			
			OutputFormat outputFormat = new OutputFormat();
			String formattedOutput = outputFormat.formatOutput(outputCollector);
			byte[] outputForDFS = formattedOutput.getBytes("UTF-8");
			
//			Upload the outputfile into DFS !

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
