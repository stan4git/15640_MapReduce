/**
 * 
 */
package mapred;

/**
 * @author menglonghe
 * @author sidilin
 *
 */
public class RMIServiceInfo {
	
	private Integer dataNodeRegPort;
	private String dataNodeService;
	private Integer partitionNums;
	private String partitionFilePath;
	private Integer taskTrackerRegPort;
	private String taskTrackServiceName;
	
	
	public void settingForMapper (Integer dataNodeRegPort, String dataNodeService, Integer partitionNums, String partitionFilePath) {
		
		this.dataNodeRegPort = dataNodeRegPort;
		this.dataNodeService = dataNodeService;
		this.partitionNums = partitionNums;
		this.partitionFilePath = partitionFilePath;	
	}
	
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
