/**
 * 
 */
package info;

import java.util.HashSet;

/**
 * @author menglonghe
 * @author sidilin
 */

public class NodeInfo {
	
	private boolean status;
	private HashSet<Integer> mapTasks;
	private HashSet<Integer> reduceTasks;
	
	public boolean isStatus() {
		return status;
	}
	public void setStatus(boolean status) {
		this.status = status;
	}
	public HashSet<Integer> getMapTasks() {
		return mapTasks;
	}
	public void setMapTasks(HashSet<Integer> mapTasks) {
		this.mapTasks = mapTasks;
	}
	public HashSet<Integer> getReduceTasks() {
		return reduceTasks;
	}
	public void setReduceTasks(HashSet<Integer> reduceTasks) {
		this.reduceTasks = reduceTasks;
	}
}
