package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

import util.NodeStatus;
import util.FileStatus;

public interface NameNodeInterface extends Remote {
	public ConcurrentHashMap<String, FileStatus> getFileStatusTable() throws RemoteException;
	public ConcurrentHashMap<String, Integer> getDataNodeAvailableSlotList() throws RemoteException;
	public ConcurrentHashMap<String, NodeStatus> getDataNodeStatusList() throws RemoteException;
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFileDistributionTable() throws RemoteException;
//	public ConcurrentHashMap<String, ConcurrentHashMap<Integer, HashSet<String>>> getFileDistributionTable(String filename) throws RemoteException;
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(
			String filename, int chunkAmount) throws RemoteException;
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> generateChunkDistributionList(
			ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> failureList) throws RemoteException;

	public boolean fileDistributionConfirm(String filename) throws RemoteException;
	public void updateFileDistributionTable(ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> tableToBeUpdated) throws RemoteException;
	public void removeChunkFromFileDistributionTable(String filename, int chunkNum, String dataNodeIP) throws RemoteException;
	public void registerDataNode(String dataNodeIP, int availableSlot) throws RemoteException;
	
//	public void chunkCopyMadeConfirm(String filename, int chunkNum, String fromIP) throws RemoteException;
	public HashSet<String> getHealthyNodes() throws RemoteException;
	public boolean fileExist(String filename) throws RemoteException;
	
	public void terminate() throws RemoteException;
}
