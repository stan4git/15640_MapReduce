package mapred;

import java.io.Serializable;

/**
 * This class is a bean which was used to record the necessary parameters 
 * for running the map reduce job. It includes mapper class, reducer class,
 * inputfile, outputfile, inputFormat and outputFormat
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public class JobConfiguration implements Serializable{

	private static final long serialVersionUID = 3646892553272150095L;
	/**
	 * The configuration parameters : Mapper class
	 * input file, output file, input Format and output Format
	 */
	private Class<?> mapperClass;
	/**
	 * The configuration parameters: Reducer class
	 */
	private Class<?> reducerClass;
	/**
	 * The configuration parameters: inputfile
	 */
	private String inputfile;
	/**
	 * The configuration parameters: outputfile
	 */
	private String outputfile;
	/**
	 * The configuration parameters: inputFormat
	 */
	private Class<?> inputFormat;
	/**
	 * The configuration parameters: outputFormat
	 */
	private Class<?> outputFormat;
	
	
	public Class<?> getMapperClass() {
		return mapperClass;
	}
	public void setMapperClass(Class<?> mapperClass) {
		this.mapperClass = mapperClass;
	}
	public Class<?> getReducerClass() {
		return reducerClass;
	}
	public void setReducerClass(Class<?> reducerClass) {
		this.reducerClass = reducerClass;
	}
	public String getInputfile() {
		return inputfile;
	}
	public void setInputfile(String inputfile) {
		this.inputfile = inputfile;
	}
	public String getOutputfile() {
		return outputfile;
	}
	public void setOutputfile(String outputfile) {
		this.outputfile = outputfile;
	}
	public Class<?> getInputFormat() {
		return inputFormat;
	}
	public void setInputFormat(Class<?> inputFormat) {
		this.inputFormat = inputFormat;
	}
	public Class<?> getOutputFormat() {
		return outputFormat;
	}
	public void setOutputFormat(Class<?> outputFormat) {
		this.outputFormat = outputFormat;
	}
}
