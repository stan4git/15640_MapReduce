package mapred;

import java.util.ArrayList;
import java.util.HashMap;

public class TaskInfo {
	
	private int jobID; 
	private int partitionNo;
	private HashMap<String, ArrayList<String>> nodesWithPartitions;
	private String className;
	private RMIServiceInfo rmiServiceInfo;
	private String outputFileName;
	
	public TaskInfo(int jobID, int partitionNo,
			HashMap<String, ArrayList<String>> nodesWithPartitions,
			String className, RMIServiceInfo rmiServiceInfo, String outputFileName) {
		
		this.jobID = jobID;
		this.partitionNo = partitionNo;
		this.nodesWithPartitions = nodesWithPartitions;
		this.className = className;
		this.rmiServiceInfo = rmiServiceInfo;
		this.outputFileName = outputFileName;
	}

	public int getJobID() {
		return jobID;
	}

	public void setJobID(int jobID) {
		this.jobID = jobID;
	}

	public int getPartitionNo() {
		return partitionNo;
	}

	public void setPartitionNo(int partitionNo) {
		this.partitionNo = partitionNo;
	}

	public HashMap<String, ArrayList<String>> getNodesWithPartitions() {
		return nodesWithPartitions;
	}

	public void setNodesWithPartitions(
			HashMap<String, ArrayList<String>> nodesWithPartitions) {
		this.nodesWithPartitions = nodesWithPartitions;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public RMIServiceInfo getRmiServiceInfo() {
		return rmiServiceInfo;
	}

	public void setRmiServiceInfo(RMIServiceInfo rmiServiceInfo) {
		this.rmiServiceInfo = rmiServiceInfo;
	}

	public String getOutputFileName() {
		return outputFileName;
	}

	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	}
}
