/**
 * 
 */
package mapred;

import java.io.Serializable;

/**
 * This class is used to reserve the RMI related info
 * including dataNode's Registry port, service name, partition numbers,
 * partition file path, taskNode's registry port and service name.
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public class RMIServiceInfo implements Serializable{

	private static final long serialVersionUID = -6852568401367174398L;
	// datanode's registry port and service name
	private Integer dataNodeRegPort;
	private String dataNodeService;
	// partition number
	private Integer partitionNums;
	// partition file path
	private String partitionFilePath;
	// task node's registry port and service name
	private Integer taskTrackerRegPort;
	private String taskTrackServiceName;
	
	/**
	 * This method is used to set the info for mapper
	 * @param dataNodeRegPort
	 * @param dataNodeService
	 * @param partitionNums
	 * @param partitionFilePath
	 */
	public void settingForMapper (Integer dataNodeRegPort, String dataNodeService, Integer partitionNums, String partitionFilePath) {
		
		this.dataNodeRegPort = dataNodeRegPort;
		this.dataNodeService = dataNodeService;
		this.partitionNums = partitionNums;
		this.partitionFilePath = partitionFilePath;	
	}
	
	/**
	 * This method is used to set the info for the reducer
	 * @param taskTrackerRegPort
	 * @param taskTrackServiceName
	 */
	public void settingForReducer (Integer taskTrackerRegPort, String taskTrackServiceName) {
		
		this.taskTrackerRegPort = taskTrackerRegPort;
		this.taskTrackServiceName = taskTrackServiceName;
	}

	public Integer getDataNodeRegPort() {
		return dataNodeRegPort;
	}

	public void setDataNodeRegPort(Integer dataNodeRegPort) {
		this.dataNodeRegPort = dataNodeRegPort;
	}

	public String getDataNodeService() {
		return dataNodeService;
	}

	public void setDataNodeService(String dataNodeService) {
		this.dataNodeService = dataNodeService;
	}

	public Integer getPartitionNums() {
		return partitionNums;
	}

	public void setPartitionNums(Integer partitionNums) {
		this.partitionNums = partitionNums;
	}

	public String getPartitionFilePath() {
		return partitionFilePath;
	}

	public void setPartitionFilePath(String partitionFilePath) {
		this.partitionFilePath = partitionFilePath;
	}

	public Integer getTaskTrackerRegPort() {
		return taskTrackerRegPort;
	}

	public void setTaskTrackerRegPort(Integer taskTrackerRegPort) {
		this.taskTrackerRegPort = taskTrackerRegPort;
	}

	public String getTaskTrackServiceName() {
		return taskTrackServiceName;
	}

	public void setTaskTrackServiceName(String taskTrackServiceName) {
		this.taskTrackServiceName = taskTrackServiceName;
	}
}
