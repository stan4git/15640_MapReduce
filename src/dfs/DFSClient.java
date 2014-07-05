package dfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import util.FileStatus;
import util.IOUtil;
import util.StringHandling;

/**
 * 1. put file to dfs.
 * 2. list file
 * 3. list node
 * 4. delete file on dfs
 * 5. calculate split/offset
 * 6. collect data receiving status
 * 7. call other datanode's heart beat
 * 8. get file
 */
public class DFSClient implements DFSClientInterface {
	private int maxChunkSlot;
	private int maxChunkSize;
	private String nameNodeIP;
	private int nameNodeRegPort;
	private int nameNodePort;
	private String nameNodeService;
	private int dataNodeRegPort;
	private int dataNodePort;
	private String dataNodeService;
	private int replicaNum;
	private int heartbeatCheckThreshold;
	private int heartbeatInterval;
	private String dataNodePath;
	private String checkPointPath;
	private int chunkTranferRetryThreshold;
	private int ackTimeout;
	private Registry nameNodeRegistry;
	private NameNodeInterface nameNode;
	private ConcurrentHashMap<String, DataNodeInterface> dataNodeList;
	private ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> dispatchList;
	
	
	public DFSClient() {
		try {
			this.nameNodeRegistry = LocateRegistry.getRegistry(nameNodeIP, nameNodeRegPort);
			this.nameNode = (NameNodeInterface) nameNodeRegistry.lookup(nameNodeService);
			System.out.println("Connected to name node.");
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {
		DFSClient client = new DFSClient();
		System.out.println("Loading configuration data...");
		IOUtil.readConf("conf/dfs.conf", client);
		System.out.println("Configuration data loaded successfully...");
		
		System.out.println("For more information, please use: \"help\"");
		System.out.println("Type in your command:");
		while (true) {
			System.out.print(">");
			
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String command;
			try {
				command = br.readLine();
			} catch (IOException e) {
				continue;
			}
			
			String[] cmdSplit = command.trim().split(" ");
			if (cmdSplit.length > 0) {
				switch (cmdSplit[0]) {
					
				}
			}
		}
	}
	
	/**
	 * Get the file list from NameNode
	 */
	private void getFileList() {
		Map<String, FileStatus> list = this.nameNode.getFullFileStatusList();
		System.out.println("Fetching file list from remote server...");
		System.out.println("Files on DFS are:");
		System.out.println("=======================Start of list=======================");
		for (Entry<String, FileStatus> row : list.entrySet()) {
			System.out.println(row.getKey() + "	" + row.getValue());
		}
		System.out.println("=======================End of list=======================");
		return;
	}
	
	/**
	 * Get the node list from NameNode
	 */
	private void getNodeList() {
		ConcurrentHashMap<String, HashSet<String>> list = this.nameNode.getFullNodeList();
		System.out.println("Fetching data node list from remote server...");
		System.out.println("Data nodes in DFS are:");
		System.out.println("=======================Start of list=======================");
		for (Entry<String, HashSet<String>> row : list.entrySet()) {
			System.out.print(row.getKey() + "	" + row.getValue().toString());
		}
		System.out.println("=======================End of list=======================");
		return;
	}
	
	
	/**
	 * Put a file from local to DFS.
	 * @param input String The path of input file.
	 * @param output String The path of output on DFS.
	 */
	private void putFile(String filePath) {
		String filename = StringHandling.getFileNameFromPath(filePath);
		ArrayList<Long> split = calculateFileSplit(filePath);
		
		//get dispatching list from name node
		dispatchList = this.nameNode.generateChunkDistributionList(filename, split.size());
		if (dispatchList != null && dispatchList.size() > 0) {
			dispatchChunks(filePath, split);
			dispatchList = null;
			System.out.println(filePath + " has been sucessfully uploaded to DFS.");
		} else {
			System.out.println("File dispatch error. Please try again.");
		}
		return;
	}
	
	
	/**
	 * Get a file from DFS.
	 * @param file String The path of input file on DFS.
	 */
	private void getFile(String filename, String output) {
		ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> fileDistribution = this.nameNode.getFileDistributionTable(filename);
		if (fileDistribution.contains(filename)) {
			for (Entry<Integer, HashSet<String>> chunkTuple : fileDistribution.get(filename).entrySet()) {
				int chunkNum = chunkTuple.getKey();
				byte[] chunk = null;
				for (String dataNodeIP : chunkTuple.getValue()) {
					//Setup remote services of data nodes
					DataNodeInterface dataNode = connectToDataNode(dataNodeIP);
					try {
						chunk = dataNode.getFile(filename);
						IOUtil.writeBinary(chunk, output);
						break;
					} catch (RemoteException e) {
						System.err.println("Exception occurs when downloading file...");
						IOUtil.deleteFile(output);
						return;
					}
				}
			}
		}
		return;
	}
	
	
	/**
	 * Delete a file on DFS.
	 * @param file String The path of file to be deleted.
	 */
	private void removeFile(String filename) {
		ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> fileDistribution = this.nameNode.getFileDistributionTable(filename);
		if (fileDistribution.contains(filename)) {
			for (Entry<Integer, HashSet<String>> chunkTuple : fileDistribution.get(filename).entrySet()) {
				int chunkNum = chunkTuple.getKey();
				byte[] chunk = null;
				for (String dataNodeIP : chunkTuple.getValue()) {
					//Setup remote services of data nodes
					DataNodeInterface dataNode = connectToDataNode(dataNodeIP);
					
					try {
						dataNode.removeFile(filename);
						
					} catch (RemoteException e) {
						System.err.println("Exception occurs when removing files. Please try again.");
						return;
					}
				}
			}
		}
	}
	
	
	/**
	 * Generate a list of split offset of the input file.
	 * @param file The path of input file.
	 * @return A list of offset of input file.
	 */
	private ArrayList<Long> calculateFileSplit(String filename) {
		return null;
	}
	
	
	/**
	 * Dispatch file chunks to data nodes per name node's instruction. The Client is responsible
	 * for guaranteeing the succeed of transfer. Whenever a chunk is successfully transfered, 
	 * the client should receive an acknowledge from the data node. In the case when failures happened,
	 * the client will first try to re-send the file chunk. After retried three times, the client will send back 
	 * the rest of the list to name node for re-allocation and try to dispatch again.
	 * @param dispatchList A list provided by NameNode towards dispatching file chunks.
	 */
	private void dispatchChunks(String filePath, ArrayList<Long> splitStartPointList) {
		String filename = StringHandling.getFileNameFromPath(filePath);
		RandomAccessFile file;
		byte[] chunk;
		
		try {
			file = new RandomAccessFile(filePath, "r");
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			System.err.println("File not found.");
			return;
		}
		
		//guaranteed to dispatch all the chunks. if failed, get new dispatch list and dispatch again
		while (dispatchList.get(filename).size() > 0) {
			for (Entry<Integer, HashSet<String>> chunkTuple : dispatchList.get(filename).entrySet()) {
				int chunkNum = chunkTuple.getKey();
				boolean success = false;
				int chunkSize = 0;
				
				//obtain the chuck to be sent
				try {
					chunkSize = (chunkNum == splitStartPointList.size() - 1) ?
							(int) (file.length() - splitStartPointList.get(chunkNum)) : 
							(int) (splitStartPointList.get(chunkNum + 1) - splitStartPointList.get(chunkNum));
					chunk = IOUtil.readChunk(file, splitStartPointList.get(chunkNum), chunkSize);
				} catch (IOException e1) {
					e1.printStackTrace();
					System.err.println("IO exception occurs when fetching chunk" + chunkNum);
					continue;
				}
				
				
				for (String dataNodeIP : chunkTuple.getValue()) {
					int retryThreshold = this.chunkTranferRetryThreshold;	//limit the times of retry
					
					//Setup remote services of data nodes
					DataNodeInterface node = connectToDataNode(dataNodeIP);
					
					//start transferring chunk. Retry if fails.
					while (success || retryThreshold > 0) {
						try {
							node.uploadChunk(filename, chunk);
							success = true;
						
							//waiting for dataNode acknowledge
							long timeoutExpiredMs = System.currentTimeMillis() + (this.ackTimeout * 1000);
							while (this.dispatchList.get(filename).get(chunkNum).contains(dataNodeIP)) {
								if (System.currentTimeMillis() >= timeoutExpiredMs)
									break;
								this.wait(1 * 1000);
							}
							
							//check if data node acknowledged received
							if (this.dispatchList.get(filename).get(chunkNum).contains(dataNodeIP)) {
								System.out.println("Upload timeout. Retrying for " 
										+ (this.chunkTranferRetryThreshold - retryThreshold + 1) + " times...");
								retryThreshold--;
							}
						} catch (RemoteException e) {
							e.printStackTrace();
							System.out.println("Exception occurs when uploading file. Retrying for " 
									+ (this.chunkTranferRetryThreshold - retryThreshold + 1) + " times...");
							retryThreshold--;
						} catch (InterruptedException e) {
							System.err.println("Timer error.");
						}
					}
					
					//print out error message
					if (retryThreshold == 0) {
						System.err.print("Upload chunk" + chunkNum + " to " + dataNodeIP + " failed.");
					}
				}
			}
			
			//Send back failure list to name node for new dispatching list.
			if (dispatchList.get(filename).size() == 0) {
				this.dispatchList = null;
			} else {
				this.dispatchList = nameNode.generateChunkDistributionList(this.dispatchList);
			}
		}
	}
	
	
	private DataNodeInterface connectToDataNode(String dataNode) {
		//Setup remote services of data nodes
		DataNodeInterface node = dataNodeList.get(dataNode);
		if (node == null) {
			try {
				Registry dataNodeRegistry = LocateRegistry.getRegistry(dataNode, dataNodeRegPort);
				node = (DataNodeInterface) dataNodeRegistry.lookup(dataNodeService);
				dataNodeList.put(dataNode, node);
			} catch (RemoteException e) {
				e.printStackTrace();
				System.out.println("Exception occurs when connecting to "+ dataNode);
			} catch (NotBoundException e) {
				e.printStackTrace();
				System.out.println("Service \"" + dataNodeService + "\" is not provided by " + dataNode);
			}
		}
		return node;
	}
	
	public void receivedACK(String fromIP, String filename, String chunkNum) {
		if (this.dispatchList != null) {
			if (this.dispatchList.contains(filename)) {
				if (this.dispatchList.get(filename).contains(chunkNum)) {
					if (this.dispatchList.get(filename).get(chunkNum).contains(fromIP)) {
						if (this.dispatchList.get(filename).get(chunkNum).size() == 1) {
							this.dispatchList.get(filename).remove(chunkNum);
						} else {
							this.dispatchList.get(filename).get(chunkNum).remove(fromIP);
						}
						return;
					}
				}
			}
		}
		System.err.println("Dispatch record not found.");
	}
}
