package dfs;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
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

public class DataNode extends UnicastRemoteObject implements DataNodeInterface {

	private static final long serialVersionUID = 7965875955130649094L;
	private Integer clientRegPort;
	private String clientServiceName;
	private Integer maxChunkSlot;
	private Integer dataNodeRegPort;
	private Integer dataNodePort;
	private String dataNodeService;
	private String dataNodePath;
	private Integer availableChunkSlot;
	private String nameNodeIP;
	private Integer nameNodeRegPort;
	private String nameNodeService;
	private Registry nameNodeRegistry;
	private NameNodeInterface nameNode;
	private Registry dataNodeRegistry;
	private Hashtable<String, DataNodeInterface> dataNodeList = new Hashtable<String, DataNodeInterface>();
	private ConcurrentHashMap<String, HashSet<Integer>> fileList = new ConcurrentHashMap<String, HashSet<Integer>>();
	private boolean isRunning;
	//private int ackTimeout;
	private int reservedSlot;
	
	public static void main(String[] args) {
		System.out.println("Starting data node server...");
		DataNode dataNode = null;
		try {
			dataNode = new DataNode();
		} catch (RemoteException e2) {
			e2.printStackTrace();
			System.exit(-1);
		}
		
		//read configuration file
		System.out.println("Loading configuration data...");
		try {
			IOUtil.readConf(IOUtil.confPath, dataNode);
			System.out.println("Configuration data loaded successfully...");
		} catch (IOException e1) {
			e1.printStackTrace();
			System.out.println("Loading configuration failed...");
			System.exit(-1);
		}
		
		try {	//set up registry
			System.out.println("Setting up registry server...");
			unexportObject(dataNode, false);
			DataNodeInterface stub = (DataNodeInterface) exportObject(dataNode, dataNode.dataNodePort);
			Registry dataNodeRegistry = LocateRegistry.createRegistry(dataNode.dataNodeRegPort);
			dataNodeRegistry.rebind(dataNode.dataNodeService, stub);
		} catch (RemoteException e) {
			e.printStackTrace();
			System.err.println("System initialization failed...");
			System.exit(-1);
		}
		
		dataNode.init();
		
		System.out.println("System is running...");
		while (dataNode.isRunning) {
			//doing nothing, just waiting
		}

		//shutting down
		System.out.println("System is shutting down...");
	}
	
	
	public DataNode() throws RemoteException {
		this.isRunning = true;
	}
	
	public void init() {
		this.availableChunkSlot = this.maxChunkSlot;
		try {
			System.out.println("Connecting to name node...");
			this.nameNodeRegistry = LocateRegistry.getRegistry(this.nameNodeIP, this.nameNodeRegPort);
			this.nameNode = (NameNodeInterface) this.nameNodeRegistry.lookup(this.nameNodeService);
			this.nameNode.registerDataNode(InetAddress.getLocalHost().getHostAddress(), this.availableChunkSlot);
		} catch (RemoteException | NotBoundException | UnknownHostException e) {
			e.printStackTrace();
			System.out.println("Cannot connect to name node...");
			System.exit(-1);
		}
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
				System.out.println("\"" + filename + "" + "_" + chunkNum + "\" has been stored to " + "\"" + this.dataNodePath + "\".");
			} catch (IOException e) {
				this.availableChunkSlot++;
				this.reservedSlot--;
				e.printStackTrace();
				System.err.println("IO exception occuring when writing files...");
				throw new RemoteException("IO exception occuring when writing files...");
			}
			
			try {	
				System.out.println("Connecting to " + fromIP + "...");
				Registry clientRegistry = LocateRegistry.getRegistry(fromIP, this.clientRegPort);		
				DFSClientInterface client = (DFSClientInterface) clientRegistry.lookup(this.clientServiceName);
				client.sendChunkReceivedACK(InetAddress.getLocalHost().getHostAddress(), filename, chunkNum);	//send out ack to client
				System.out.println("Client " + fromIP + " acknowledged.");
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
		try {
			IOUtil.deleteFile(this.dataNodePath + filename + "_" + chunkNum);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Cannot remove " + filename + "_" + chunkNum + "...");
			return;
		}
		availableChunkSlot++;
		System.out.println(filename + "_" + chunkNum + " has been deleted from storage...");
		return;
	}

	
	public byte[] getFile(String filename, int chunkNum) throws RemoteException {
		byte[] chunk;
		try {
			chunk = IOUtil.readFile(this.dataNodePath + filename + "_" + chunkNum);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Cannot fetch file " + filename + "_" + chunkNum + "...");
			throw (new RemoteException());
		}
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


	public void downloadChunk(String filename, int chunkNum, String fromIP) throws RemoteException {
		if (!this.dataNodeList.contains(fromIP)) {		//cache connection to other data nodes
			try {
				Registry dataNodeRegistry = LocateRegistry.getRegistry(fromIP, this.dataNodeRegPort);
				DataNodeInterface dataNode = (DataNodeInterface) dataNodeRegistry.lookup(dataNodeService);
				this.dataNodeList.put(fromIP, dataNode);
			} catch (RemoteException | NotBoundException e) {
				e.printStackTrace();
				System.out.println("Cannot connect to client " + fromIP);
				throw (new RemoteException());
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
			throw (new RemoteException());
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
