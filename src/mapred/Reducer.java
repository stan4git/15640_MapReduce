/**
 * 
 */
package mapred;

import java.util.ArrayList;

import format.OutputCollector;

/**
 * @author menglonghe
 * @author sidilin
 *
 */
public interface Reducer {

	public void reduce(String key, ArrayList<String> value, OutputCollector outputCollector); 

}
