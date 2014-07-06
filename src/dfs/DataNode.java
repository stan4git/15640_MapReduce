package dfs;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

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
	private int replicaNum;
	private int heartbeatCheckThreshold;
	private int heartbeatInterval;
	private String dataNodePath;
	private String checkPointPath;
	private int chunkTranferRetryThreshold;
	private int ackTimeout;
	private Registry dataNodeRegistry;
	private Registry nameNodeRegistry;
	private NameNodeInterface nameNode;
	private int availableChunkSlot;
	
	public static void main(String[] args) {
		DataNode dataNode = new DataNode();
		System.out.println("Loading configuration data...");
		IOUtil.readConf("conf/dfs.conf", dataNode);
		System.out.println("Configuration data loaded successfully...");
		
		System.out.println("System is running...");
		while (true) {
			
		}
	}
	
	public DataNode() {
		try {
			this.availableChunkSlot = this.maxChunkSlot;
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
}
