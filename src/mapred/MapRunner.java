package mapred;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

import util.IOUtil;
import dfs.DataNodeInterface;
import format.InputFormat;
import format.KVPair;
import format.MapperOutputCollector;

public class MapRunner implements Runnable{
	private Mapper mapper;
	private Integer jobID;
	private Integer numOfChunks;
	private JobConfiguration jobConf;
	private ArrayList<KVPair> pairLists;
	private String classname;
	private Integer mapperNum;
	
	private Integer dataNodeRegPort;
	private String dataNodeService;
	private Integer partitionNums;
	private String partitionFilePath;
	
	public MapRunner (Integer jobID, Integer numOfChunks, JobConfiguration jobConf,
			ArrayList<KVPair> pairLists, String classname, Integer mapperNum, RMIServiceInfo rmiServiceInfo) {
		
		this.jobID = jobID;
		this.numOfChunks = numOfChunks;
		this.jobConf = jobConf;
		this.pairLists = pairLists;
		this.classname = classname;
		this.mapperNum = mapperNum;
		
		this.dataNodeRegPort = rmiServiceInfo.getDataNodeRegPort();
		this.dataNodeService = rmiServiceInfo.getDataNodeService();
		this.partitionNums = rmiServiceInfo.getPartitionNums();
		this.partitionFilePath = rmiServiceInfo.getPartitionFilePath();
	}
	
	

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		Class<Mapper> mapClass;
		try {
			//step1 : get the programmer's Mapper class and Instantiate it
			mapClass = (Class<Mapper>) Class.forName(classname);
			Constructor<Mapper> constructors = mapClass.getConstructor();
			mapper = constructors.newInstance();
			
			// step2: Get the chunks data, format them using the LineFormat 
			// and filling these into OutputCollector
			String contents[] = new String[numOfChunks];
			int count = 0;
			MapperOutputCollector outputCollector = new MapperOutputCollector();
			ArrayList<String> filePaths = new ArrayList<String>();
			for(KVPair pair : pairLists) {
				Integer chunkNum = (Integer) pair.getKey();
				String sourceNodeIP = (String) pair.getValue();
				Registry reg = LocateRegistry.getRegistry(sourceNodeIP, dataNodeRegPort);
				DataNodeInterface datanode = (DataNodeInterface)reg.lookup(dataNodeService);
				contents[count] = new String(datanode.getFile(jobConf.getInputfile(),chunkNum),"UTF-8");
				Class<InputFormat> inputFormatClass = (Class<InputFormat>) Class.forName(jobConf.getInputFormat().toString());
				Constructor<InputFormat> constuctor = inputFormatClass.getConstructor(String.class);
				InputFormat inputFormat = constuctor.newInstance(contents[count]);
				List<KVPair> kvPairs = inputFormat.getKvPairs();
				for(int i = 0; i < kvPairs.size(); i++) {
					mapper.map(pair.getKey(), pair.getValue(), outputCollector);
				}
				count++;
			}
			// step3: partition the OutputCollector
			StringBuffer[] partitionContents = Partitioner.partition(outputCollector.mapperOutputCollector,partitionNums);
			// step4: write the partition contents to the specific path
			for(int j = 0; j < partitionNums; j++) {
				String filename = partitionFilePath + jobID.toString() + "/" + mapperNum.toString() + "/partition" + j;
				filePaths.add(filename);
				IOUtil.writeBinary(partitionContents[j].toString().getBytes("UTF-8"), filename);
			}
			// step5: notify task tracker to update task status
			TaskTracker.updateFilePaths(jobID, filePaths);
			TaskTracker.updateMapStatus(jobID, true);
		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException 
				| InstantiationException | IllegalAccessException 
				| IllegalArgumentException | InvocationTargetException | RemoteException 
				| NotBoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
	}
}
