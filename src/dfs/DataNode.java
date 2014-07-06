package dfs;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Hashtable;

import util.IOUtil;

/**
 * 1. heart beat (RMI)
 * 2. Setup registry 
 * 3. receive file
 * 4. available chunk slot
 * 5. makeCopy for RMI call
 */
public class DataNode implements DataNodeInterface {
	private int clientPort;
	private String clientServiceName;
	private int maxChunkSlot;
	private String nameNodeIP;
	private int nameNodeRegPort;
	private int nameNodePort;
	private String nameNodeService;
	private int dataNodeRegPort;
	private int dataNodePort;
	private String dataNodeService;
	private String dataNodePath;
	private Registry dataNodeRegistry;
	private Registry nameNodeRegistry;
	private NameNodeInterface nameNode;
	private int availableChunkSlot;
	private Hashtable<String, DataNodeInterface> dataNodeList;
	private boolean isRunning;
	
	
	public static void main(String[] args) {
		DataNode dataNode = new DataNode();
		System.out.println("Loading configuration data...");
		IOUtil.readConf("conf/dfs.conf", dataNode);
		System.out.println("Configuration data loaded successfully...");
		
		System.out.println("System is running...");
		while (dataNode.isRunning) {
			
		}

		System.out.println("System is shutting down...");
	}
	
	public DataNode() {
		try {
			isRunning = true;
			this.availableChunkSlot = this.maxChunkSlot;
			dataNodeList = new Hashtable<String, DataNodeInterface>();
			dataNodeRegistry = LocateRegistry.createRegistry(this.dataNodeRegPort);
			dataNodeRegistry.bind(dataNodeService, this);
			System.out.println("Server has been set up...");
			
			this.nameNodeRegistry = LocateRegistry.getRegistry(nameNodeIP, nameNodePort);
			this.nameNode = (NameNodeInterface) nameNodeRegistry.lookup(nameNodeService);
			System.out.println("Connected to name node.");
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
		}
	}
	
	public void uploadChunk(String filename, byte[] chunk, int chunkNum, String fromIP)
			throws RemoteException {
		try {
			if (availableChunkSlot <= 0) {
				throw new RemoteException("Storage is full!! Please try another data node.");
			}
			IOUtil.writeBinary(chunk, this.dataNodePath + filename + "_" + chunkNum);
			availableChunkSlot--;
			System.out.println(filename + "_" + chunkNum + " has been stored to " + this.dataNodePath);
			
			Registry clientRegistry = LocateRegistry.getRegistry(fromIP, this.clientPort);
			DFSClientInterface client = (DFSClientInterface) clientRegistry.lookup(clientServiceName);
			client.sendACK(InetAddress.getLocalHost().getHostAddress(), filename, chunkNum);
			System.out.println("Client acknowledged.");
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("IO exception occuring when writing files...");
			throw new RemoteException("IO exception occuring when writing files...");
		} catch (NotBoundException e) {
			System.err.println("Unable to inform client server...");
			e.printStackTrace();
		}
	}

	public void removeFile(String filename, int chunkNum) throws RemoteException {
		if (availableChunkSlot >= this.maxChunkSlot) {
			throw new RemoteException("There is no file on this node.");
		}
		IOUtil.deleteFile(this.dataNodePath + filename + "_" + chunkNum);
		availableChunkSlot++;
		System.out.println(filename + "_" + chunkNum + " has been deleted from storage...");
		return;
	}


	@Override
	public byte[] getFile(String filename, int chunkNum) throws RemoteException {
		byte[] chunk = IOUtil.readFile(this.dataNodePath + filename + "_" + chunkNum);
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
	public void downloadChunk(String filename, int chunkNum, String fromIP) {
		if (!this.dataNodeList.contains(fromIP)) {
			try {
				Registry dataNodeRegistry = LocateRegistry.getRegistry(fromIP, this.dataNodePort);
				DataNodeInterface dataNode = (DataNodeInterface) dataNodeRegistry.lookup(dataNodeService);
				this.dataNodeList.put(fromIP, dataNode);
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			}
		}
		
		try {
			byte[] chunk = this.dataNodeList.get(fromIP).getFile(filename, chunkNum);
			IOUtil.writeBinary(chunk, dataNodePath + filename + "_" + chunkNum);
			System.out.println(filename + "_" + chunkNum + " has been downloaded...");
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void terminate() {
		this.isRunning = false;
	}
}
