/**
 * 
 */
package info;

import mapred.JobConfiguration;

/**
 * @author menglonghe
 * @author sidilin
 */

public class JobInfo {
	
	private boolean status;
	private int failTimes;
	private String mapperClassPath;
	private String reducerClassPath;
	private String mapperClassName;
	private String reducerClassName;
	private JobConfiguration jobConfig;
	
	public boolean isStatus() {
		return status;
	}
	public void setStatus(boolean status) {
		this.status = status;
	}
	public int getFailTimes() {
		return failTimes;
	}
	public void setFailTimes(int failTimes) {
		this.failTimes = failTimes;
	}
	public String getMapperClassPath() {
		return mapperClassPath;
	}
	public void setMapperClassPath(String mapperClassPath) {
		this.mapperClassPath = mapperClassPath;
	}
	public String getReducerClassPath() {
		return reducerClassPath;
	}
	public void setReducerClassPath(String reducerClassPath) {
		this.reducerClassPath = reducerClassPath;
	}
	public String getMapperClassName() {
		return mapperClassName;
	}
	public void setMapperClassName(String mapperClassName) {
		this.mapperClassName = mapperClassName;
	}
	public String getReducerClassName() {
		return reducerClassName;
	}
	public void setReducerClassName(String reducerClassName) {
		this.reducerClassName = reducerClassName;
	}
	public JobConfiguration getJobConfig() {
		return jobConfig;
	}
	public void setJobConfig(JobConfiguration jobConfig) {
		this.jobConfig = jobConfig;
	}
}
