package dfs;

import java.rmi.RemoteException;

/**
 * 1. heart beat (RMI)
 * 2. Setup registry 
 * 3. receive file
 * 4. available chunk slot
 * 5. makeCopy for RMI call
 */
public class DataNode implements DataNodeInterface {

	@Override
	public void uploadChunk(String filename, byte[] chunk)
			throws RemoteException {
	}
	
}
