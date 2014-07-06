package mapred;

import java.util.ArrayList;

import format.KVPair;

/**
 * 1. wrap mapper into thread
 * 2. write partition to patrition path
 * 
 */
public class MapRunner {
	private Mapper mapper;
	private Integer jobID;
	private Integer numOfChunks;
	private JobConfiguration jobConf;
	private ArrayList<KVPair> pairLists;
	private Integer regPort;
	private String serviceName;
	private String classname;
	private String partitionPath;
	private int numPartitions;
	
	public MapRunner(Integer jobID, Integer numOfChunks, JobConfiguration jobConf,
			ArrayList<KVPair> pairLists, Integer regPort,
			String serviceName, String classname, String partitionPath, Integer numPartitions){
		this.jobID = jobID;
		this.numOfChunks = numOfChunks;
		this.jobConf = jobConf;
		this.pairLists = pairLists;
		this.regPort = regPort;
		this.serviceName = serviceName;
		this.classname = classname;
		this.partitionPath = partitionPath;
		this.numPartitions = numPartitions;
	}
	
	public void start() {
		
	}
}
