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
	
	private String jobTrackerIP;
	private Integer jobTrackerRegPort;
	private String jobTrackServiceName;
	private Integer dataNodeRegPort;
	private String dataNodeService;
	private Integer partitionNums;
	private String partitionFilePath;
	private String nameNodeIP;
	private Integer nameNodeRegPort;
	private String nameNodeService;
	private Integer taskTrackerRegPort;
	private String taskTrackServiceName;
	private String jobOutputPath;
	
	public void settingForMapper (Integer dataNodeRegPort, String dataNodeService, Integer partitionNums, String partitionFilePath) {
		
		this.dataNodeRegPort = dataNodeRegPort;
		this.dataNodeService = dataNodeService;
		this.partitionNums = partitionNums;
		this.partitionFilePath = partitionFilePath;	
	}
	
	public void settingForReducer (String nameNodeIp, Integer nameNodeRegPort, String nameNodeService, Integer taskTrackerRegPort,
			String taskTrackServiceName, String jobOutputPath) {
		
		this.nameNodeIP = nameNodeIp;
		this.nameNodeRegPort = nameNodeRegPort;
		this.nameNodeService = nameNodeService;
		this.taskTrackerRegPort = taskTrackerRegPort;
		this.taskTrackServiceName = taskTrackServiceName;
		this.jobOutputPath = jobOutputPath;
		
	}

	public String getJobTrackerIP() {
		return jobTrackerIP;
	}

	public void setJobTrackerIP(String jobTrackerIP) {
		this.jobTrackerIP = jobTrackerIP;
	}

	public Integer getJobTrackerRegPort() {
		return jobTrackerRegPort;
	}

	public void setJobTrackerRegPort(Integer jobTrackerRegPort) {
		this.jobTrackerRegPort = jobTrackerRegPort;
	}

	public String getJobTrackServiceName() {
		return jobTrackServiceName;
	}

	public void setJobTrackServiceName(String jobTrackServiceName) {
		this.jobTrackServiceName = jobTrackServiceName;
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

	public String getNameNodeIP() {
		return nameNodeIP;
	}

	public void setNameNodeIP(String nameNodeIP) {
		this.nameNodeIP = nameNodeIP;
	}

	public Integer getNameNodeRegPort() {
		return nameNodeRegPort;
	}

	public void setNameNodeRegPort(Integer nameNodeRegPort) {
		this.nameNodeRegPort = nameNodeRegPort;
	}

	public String getNameNodeService() {
		return nameNodeService;
	}

	public void setNameNodeService(String nameNodeService) {
		this.nameNodeService = nameNodeService;
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

	public String getJobOutputPath() {
		return jobOutputPath;
	}

	public void setJobOutputPath(String jobOutputPath) {
		this.jobOutputPath = jobOutputPath;
	}
		
}
