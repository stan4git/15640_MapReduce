package dfs;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

import util.IOUtil;

/**
 * 1. heart beat (RMI)
 * 2. Setup registry 
 * 3. receive file
 * 4. available chunk slot
 * 5. makeCopy for RMI call
 */
public class DataNode implements DataNodeInterface {
	private Integer clientPort;
	private String clientServiceName;
	private Integer maxChunkSlot;
	private Integer dataNodeRegPort;
	private Integer dataNodePort;
	private String dataNodeService;
	private String dataNodePath;
	private Integer availableChunkSlot;
	private String nameNodeIP;
	private int nameNodePort;
	private String nameNodeService;
	private Registry nameNodeRegistry;
	private NameNodeInterface nameNode;
	private Hashtable<String, DataNodeInterface> dataNodeList;
	private ConcurrentHashMap<String, HashSet<Integer>> fileList;
	private boolean isRunning;
	private int ackTimeout;
	private int reservedSlot;
	
	public static void main(String[] args) {
		Registry dataNodeRegistry;
		DataNode dataNode;
		try {
			dataNode = new DataNode();
			System.out.println("Configuring server...");
			dataNodeRegistry = LocateRegistry.createRegistry(dataNode.dataNodeRegPort);
			dataNodeRegistry.rebind(dataNode.dataNodeService, dataNode);
		} catch (RemoteException e) {
			e.printStackTrace();
			System.err.println("System initialization failed...");
			return;
		}
		
		System.out.println("System is running...");
		while (dataNode.isRunning) {
			
		}

		System.out.println("System is shutting down...");
	}
	
	
	public DataNode() throws RemoteException {
		this.isRunning = true;
		this.availableChunkSlot = this.maxChunkSlot;
		this.dataNodeList = new Hashtable<String, DataNodeInterface>();
		this.fileList = new ConcurrentHashMap<String, HashSet<Integer>>();
		
		System.out.println("Loading configuration data...");
		IOUtil.readConf("conf/dfs.conf", this);
		System.out.println("Configuration data loaded successfully...");
		System.out.println(this.dataNodeRegPort);
		
		try {
			this.nameNodeRegistry = LocateRegistry.getRegistry(this.nameNodeIP, this.nameNodePort);
			this.nameNode = (NameNodeInterface) this.nameNodeRegistry.lookup(this.nameNodeService);
				this.nameNode.registerDataNode(InetAddress.getLocalHost().getHostAddress(), this.availableChunkSlot);
		} catch (RemoteException | NotBoundException | UnknownHostException e) {
			System.out.println("Cannot connect to name node...");
			throw new RemoteException();
		}
		return;
	}
	
	
	public void uploadChunk(String filename, byte[] chunk, int chunkNum, String fromIP)
			throws RemoteException {
			if (this.availableChunkSlot <= 0) {		//check if there is available slots
				throw new RemoteException("System storage error.");
			} else {			//reserve slots for upload
				this.availableChunkSlot--;
				this.reservedSlot++;
			}
			
			try {		//write file on to local storage
				IOUtil.writeBinary(chunk, this.dataNodePath + filename + "_" + chunkNum);		
				System.out.println(filename + "_" + chunkNum + " has been stored to " + this.dataNodePath);
			} catch (IOException e) {
				this.availableChunkSlot++;
				this.reservedSlot--;
				e.printStackTrace();
				System.err.println("IO exception occuring when writing files...");
				throw new RemoteException("IO exception occuring when writing files...");
			}
			
			try {	
				Registry clientRegistry = LocateRegistry.getRegistry(fromIP, this.clientPort);		
				DFSClientInterface client = (DFSClientInterface) clientRegistry.lookup(this.clientServiceName);
				client.sendChunkReceivedACK(InetAddress.getLocalHost().getHostAddress(), filename, chunkNum);	//send out ack to client
				System.out.println("Client acknowledged.");
			} catch (NotBoundException | UnknownHostException e) {
				e.printStackTrace();
				System.err.println("Unable to connect to client server...");
				try {
					IOUtil.deleteFile(dataNodePath + filename + "_" + chunkNum);
					this.availableChunkSlot++;
					this.reservedSlot--;
					System.out.println("Removing " + filename + "_" + chunkNum);
				} catch (IOException e1) {
					System.out.println("Exception occurs when removing" + filename + "_" + chunkNum);
				}
				return;
			}
			
			
			if (this.fileList.contains(filename)) {		//update local file list
				this.fileList.get(filename).add(chunkNum);
			} else {
				HashSet<Integer> value = new HashSet<Integer>();
				value.add(chunkNum);
				this.fileList.put(filename, value);
			}
			this.reservedSlot--;				//commit update
			return;
			
			
//			long timeoutExpiredMs = System.currentTimeMillis() + (this.ackTimeout * 1000);	
//			while (!ack) {	//waiting for client acknowledge
//				if (System.currentTimeMillis() < timeoutExpiredMs) {
//					try {
//						Thread.sleep(1 * 1000);
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//				} else {
//					IOUtil.deleteFile(this.dataNodePath + filename + "_" + chunkNum);
//					System.err.println();
//				}
//			}
	}
	

	public void removeChunk(String filename, int chunkNum) throws RemoteException {
		if (availableChunkSlot >= this.maxChunkSlot) {
			throw new RemoteException("There is no file on this node.");
		}
		IOUtil.deleteFile(this.dataNodePath + filename + "_" + chunkNum);
		availableChunkSlot++;
		System.out.println(filename + "_" + chunkNum + " has been deleted from storage...");
		return;
	}

	
	public byte[] getFile(String filename, int chunkNum) throws RemoteException {
		byte[] chunk = IOUtil.readFile(this.dataNodePath + filename + "_" + chunkNum);
		System.out.println("Sending out " + filename + "_" + chunkNum + " ...");
		return chunk;
	}


	public boolean heartbeat() throws RemoteException {
		return true;
	}


	public boolean hasChunk(String filename, int chunkNum) throws RemoteException {
		File file = new File(this.dataNodePath + filename + "_" + chunkNum);
		return file.exists();
	}


	public void downloadChunk(String filename, int chunkNum, String fromIP) {
		if (!this.dataNodeList.contains(fromIP)) {		//cache connection to other data nodes
			try {
				Registry dataNodeRegistry = LocateRegistry.getRegistry(fromIP, this.dataNodePort);
				DataNodeInterface dataNode = (DataNodeInterface) dataNodeRegistry.lookup(dataNodeService);
				this.dataNodeList.put(fromIP, dataNode);
			} catch (RemoteException | NotBoundException e) {
				e.printStackTrace();
				System.out.println("Cannot connect to client " + fromIP);
				return;
			}
		} 
		
		try {		//download chunks from other data node
			availableChunkSlot--;
			reservedSlot++;
			byte[] chunk = this.dataNodeList.get(fromIP).getFile(filename, chunkNum);
			IOUtil.writeBinary(chunk, dataNodePath + filename + "_" + chunkNum);
			System.out.println(filename + "_" + chunkNum + " has been downloaded...");
		} catch (IOException e) {
			availableChunkSlot++;
			reservedSlot--;
			e.printStackTrace();
			System.out.println("IO exception occurs when removing " + filename + "_" + chunkNum);
		}
	}


	public void terminate() {
		this.isRunning = false;
	}


	public ConcurrentHashMap<String, HashSet<Integer>> getFileChunkList() {
		return this.fileList;
	}


	@Override
	public int getAvailableChunkSlot() {
		return this.availableChunkSlot;
	}
}
