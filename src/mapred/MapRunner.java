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
import format.OutputCollector;

public class MapRunner implements Runnable{
	private Mapper mapper;
	private Integer jobID;
	private Integer numOfChunks;
	private JobConfiguration jobConf;
	private ArrayList<KVPair> pairLists;
	private Integer regPort;
	private String serviceName;
	private String classname;
	private String partitionPath;
	private Integer numPartitions;
	private Integer mapperNum;
	
	public MapRunner(Integer jobID, Integer numOfChunks, JobConfiguration jobConf,
			ArrayList<KVPair> pairLists, Integer regPort,
			String serviceName, String classname, String partitionPath, Integer numPartitions, Integer mapperNum){
		this.jobID = jobID;
		this.numOfChunks = numOfChunks;
		this.jobConf = jobConf;
		this.pairLists = pairLists;
		this.regPort = regPort;
		this.serviceName = serviceName;
		this.classname = classname;
		this.partitionPath = partitionPath;
		this.numPartitions = numPartitions;
		this.mapperNum = mapperNum;
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
			OutputCollector outputCollector = new OutputCollector();
			for(KVPair pair : pairLists) {
				Integer chunkNum = (Integer) pair.getKey();
				String sourceNodeIP = (String) pair.getValue();
				Registry reg = LocateRegistry.getRegistry(sourceNodeIP, regPort);
				DataNodeInterface datanode = (DataNodeInterface)reg.lookup(serviceName);
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
			StringBuffer[] partitionContents = Partitioner.partition(outputCollector.outputCollector,numPartitions);
			// step4: write the partition contents to the specific path
			for(int j = 0; j < numPartitions; j++) {
				String filename = jobID.toString() + mapperNum.toString() + "partition" + j;
				IOUtil.writeBinary(partitionContents[j].toString().getBytes("UTF-8"), partitionPath + filename);
			}
		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException 
				| InstantiationException | IllegalAccessException 
				| IllegalArgumentException | InvocationTargetException | RemoteException 
				| NotBoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
	}
}
