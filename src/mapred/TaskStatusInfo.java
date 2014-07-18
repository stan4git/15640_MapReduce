/**
 * 
 */
package mapred;

import java.io.Serializable;

/**
 * This class is designed to encapsulate information of task status of one slave node.
 * It contains the number of total and unfinished map the reduce tasks. 
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public class TaskStatusInfo implements Serializable{

	private static final long serialVersionUID = -58329058647737407L;
	/** total map tasks on the node of the specific job */
	private int totalMapTasks;
	/** the unfinished map tasks on the node of the specific job */
	private int unfinishedMapTasks;
	/** total reduce tasks on the node of the specific job */
	private int totalReduceTasks;
	/** the unfinished reduce tasks on the node of the specific job */
	private int unfinishedReduceTasks;
	
	public int getTotalMapTasks() {
		return totalMapTasks;
	}
	public void setTotalMapTasks(int totalMapTasks) {
		this.totalMapTasks = totalMapTasks;
	}
	public int getUnfinishedMapTasks() {
		return unfinishedMapTasks;
	}
	public void setUnfinishedMapTasks(int unfinishedMapTasks) {
		this.unfinishedMapTasks = unfinishedMapTasks;
	}
	public int getTotalReduceTasks() {
		return totalReduceTasks;
	}
	public void setTotalReduceTasks(int totalReduceTasks) {
		this.totalReduceTasks = totalReduceTasks;
	}
	public int getUnfinishedReduceTasks() {
		return unfinishedReduceTasks;
	}
	public void setUnfinishedReduceTasks(int unfinishedReduceTasks) {
		this.unfinishedReduceTasks = unfinishedReduceTasks;
	}
}
