/**
 * 
 */
package mapred;

import java.util.ArrayList;

import format.ReducerOutputCollector;

/**
 * This class is a interface of Reducer
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public interface Reducer {
	/**
	 * This method is used to wrap the key-value pairs into the 
	 * ReducerOutputCollector
	 * 
	 * @param key
	 * @param value
	 * @param outputCollector
	 */
	public void reduce(String key, ArrayList<String> value, ReducerOutputCollector outputCollector); 

}
