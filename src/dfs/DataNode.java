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
import util.PathConfiguration;

/**
 * This class is the main thread to start a data node in DFS.
 * It provides remote services for name nodes and clients to upload, query
 * data.
 */
public class DataNode extends UnicastRemoteObject implements DataNodeInterface {

	private static final long serialVersionUID = 7965875955130649094L;
	/**DFSClient RMI service name, read from dfs.conf*/
	private String clientServiceName;
	/**Maximum chunk slot in each DataNode, read from dfs.conf*/
	private Integer maxChunkSlot;
	/**Registry service port on DataNode, read from dfs.conf*/
	private Integer dataNodeRegPort;
	/**RMI service port on DataNode, read from dfs.conf*/
	private Integer dataNodePort;
	/**DataNOde RMI service name, read from dfs.conf*/
	private String dataNodeService;
	/**Storage path on DataNode, read from dfs.conf*/
	private String dataNodePath;
	/**Available chunk slots on DataNode, read from dfs.conf*/
	private Integer availableChunkSlot;
	/**NameNode IP address, read from dfs.conf*/
	private String nameNodeIP;
	/**NameNode registry service port, read from dfs.conf*/
	private Integer nameNodeRegPort;
	/**NameNode RMI service name, read from dfs.conf*/
	private String nameNodeService;
	/**NameNode RMI service remote object, created when initializing DataNode.*/
	private Registry nameNodeRegistry;
	/**RMI stub object. Cached once created.*/
	private NameNodeInterface nameNode;
	/**Current DataNode RMI service.*/
	private static Registry dataNodeRegistry;
	/**Connection cache pool of RMI services to other DataNodes.*/
	private Hashtable<String, DataNodeInterface> dataNodeList = new Hashtable<String, DataNodeInterface>();
	/**File list on this DataNode.*/
	private ConcurrentHashMap<String, HashSet<Integer>> fileList = new ConcurrentHashMap<String, HashSet<Integer>>();
	/**A flag that used to shut down this DataNode.*/
	private boolean isRunning;
	/**Slots that are assigned but not necessarily occupied.*/
	private int reservedSlot;
	
	
	public static void main(String[] args) {
		System.out.println("Starting data node server...");
		DataNode dataNode = null;
		try {
			dataNode = new DataNode();
		} catch (RemoteException e2) {
//			e2.printStackTrace();
			System.exit(-1);
		}
		
		//read configuration file
		System.out.println("Loading configuration data...");
		try {
			IOUtil.readConf(PathConfiguration.DFSConfPath, dataNode);
			System.out.println("Configuration data loaded successfully...");
		} catch (IOException e1) {
//			e1.printStackTrace();
			System.out.println("Loading configuration failed...");
			System.exit(-1);
		}
		
		try {	//set up registry
			System.out.println("Setting up registry server...");
			unexportObject(dataNode, false);
			DataNodeInterface stub = (DataNodeInterface) exportObject(dataNode, dataNode.dataNodePort);
			dataNodeRegistry = LocateRegistry.createRegistry(dataNode.dataNodeRegPort);
			dataNodeRegistry.rebind(dataNode.dataNodeService, stub);
		} catch (RemoteException e) {
//			e.printStackTrace();
			System.err.println("System initialization failed...");
			System.exit(-1);
		}
		
		//setup connections
		dataNode.init();
		
		System.out.println("System is running...");
		while (dataNode.isRunning) {
			//doing nothing, just waiting
		}

		//shutting down
		System.out.println("System is shutting down...");
	}
	
	/**
	 * While isRunning is true, this data node will keep running.
	 * Calling terminate() will change isRunning to false.
	 * @throws RemoteException
	 */
	public DataNode() throws RemoteException {
		this.isRunning = true;
	}
	
	/**
	 * Used right after system started. It creates connection to name node
	 * and retrieve services.
	 */
	public void init() {
		this.availableChunkSlot = this.maxChunkSlot;
		try {
			System.out.println("Connecting to name node...");
			this.nameNodeRegistry = LocateRegistry.getRegistry(this.nameNodeIP, this.nameNodeRegPort);
			this.nameNode = (NameNodeInterface) this.nameNodeRegistry.lookup(this.nameNodeService);
			this.nameNode.registerDataNode(InetAddress.getLocalHost().getHostAddress(), this.availableChunkSlot);
		} catch (RemoteException | NotBoundException | UnknownHostException e) {
//			e.printStackTrace();
			System.out.println("Cannot connect to name node...");
			System.exit(-1);
		}
	}
	
	
	@Override
	public void uploadChunk(String filename, byte[] chunk, int chunkNum, String fromIP, int RMIPort)
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
//				e.printStackTrace();
				System.err.println("IO exception occuring when writing files...");
				throw new RemoteException("IO exception occuring when writing files...");
			}
			
			try {	
				System.out.println("Connecting to client " + fromIP + "...");
				Registry clientRegistry = LocateRegistry.getRegistry(fromIP, RMIPort);		
				DFSClientInterface client = (DFSClientInterface) clientRegistry.lookup(this.clientServiceName);
				client.sendChunkReceivedACK(InetAddress.getLocalHost().getHostAddress(), filename, chunkNum);	//send out ack to client
				System.out.println("Client " + fromIP + " acknowledged.");
			} catch (NotBoundException | UnknownHostException e) {
//				e.printStackTrace();
				System.err.println("Unable to connect to client server...");
				try {
					IOUtil.deleteFile(dataNodePath + filename + "_" + chunkNum);
					this.availableChunkSlot++;
					this.reservedSlot--;
					System.out.println("Removing " + filename + "_" + chunkNum);
				} catch (IOException e1) {
					System.out.println("Exception occurs when removing" + filename + "_" + chunkNum);
				}
				throw new RemoteException();
			}
			
			
			if (this.fileList.containsKey(filename)) {		//update local file list
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
	
	
	@Override
	public void removeChunk(String filename, int chunkNum) throws RemoteException {
		if (availableChunkSlot >= this.maxChunkSlot) {
			throw new RemoteException("There is no file on this node.");
		}
		try {
			IOUtil.deleteFile(this.dataNodePath + filename + "_" + chunkNum);
		} catch (IOException e) {
//			e.printStackTrace();
			System.out.println("Cannot remove " + filename + "_" + chunkNum + "...");
			return;
		}
		availableChunkSlot++;
		System.out.println(filename + "_" + chunkNum + " has been deleted from storage...");
		return;
	}

	@Override
	public byte[] getFile(String filename, int chunkNum) throws RemoteException {
		byte[] chunk;
		try {
			chunk = IOUtil.readFile(this.dataNodePath + filename + "_" + chunkNum);
		} catch (IOException e) {
//			e.printStackTrace();
			System.out.println("Cannot fetch file " + filename + "_" + chunkNum + "...");
			throw (new RemoteException());
		}
		System.out.println("Sending out " + filename + "_" + chunkNum + " ...");
		return chunk;
	}

	
	@Override
	public boolean heartbeat() throws RemoteException {
		return true;
	}


	@Override
	public boolean hasChunk(String filename, int chunkNum) throws RemoteException {
		File file = new File(this.dataNodePath + filename + "_" + chunkNum);
		return file.exists();
	}


	@Override
	public void downloadChunk(String filename, int chunkNum, String fromIP) throws RemoteException {
		if (!this.dataNodeList.containsKey(fromIP)) {		//cache connection to other data nodes
			try {
				Registry dataNodeRegistry = LocateRegistry.getRegistry(fromIP, this.dataNodeRegPort);
				DataNodeInterface dataNode = (DataNodeInterface) dataNodeRegistry.lookup(dataNodeService);
				this.dataNodeList.put(fromIP, dataNode);
			} catch (RemoteException | NotBoundException e) {
//				e.printStackTrace();
				System.out.println("Cannot connect to client " + fromIP);
				throw (new RemoteException());
			}
		} 
		
		try {		//download chunks from other data node
			availableChunkSlot--;
			reservedSlot++;
			System.out.println("Reserving slots for upload. Current reserved slots: " + reservedSlot + ".");
			byte[] chunk = this.dataNodeList.get(fromIP).getFile(filename, chunkNum);
			IOUtil.writeBinary(chunk, dataNodePath + filename + "_" + chunkNum);
			System.out.println(filename + "_" + chunkNum + " has been downloaded...");
			
		} catch (IOException e) {
			availableChunkSlot++;
			reservedSlot--;
//			e.printStackTrace();
			System.out.println("IO exception occurs when removing " + filename + "_" + chunkNum);
			throw (new RemoteException());
		}
	}

	@Override
	public void terminate() {
		this.isRunning = false;
	}

	@Override
	public ConcurrentHashMap<String, HashSet<Integer>> getFileChunkList() {
		return this.fileList;
	}


	@Override
	public int getAvailableChunkSlot() {
		return this.availableChunkSlot;
	}
}
