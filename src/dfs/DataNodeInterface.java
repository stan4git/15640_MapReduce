package dfs;

import java.rmi.Remote;

public interface DataNodeInterface extends Remote {
	public void uploadChunk();
}
