package format;

import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This class defines a structure which was used to collect the Mapper and Reducer's 
 * intermediate keys and values. These keys and values will be sorted by keys and 
 * merge their values into a ArrayList. 
 * 
 * It also provide an add() which was used to add single key and value into this object.
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public class ReducerOutputCollector {
	
	// such as <Hello,<1,1,1,1,1>>
	public SortedMap<Object,ArrayList<Object>> outputCollector = new TreeMap<Object,ArrayList<Object>>();

	/**
	 * This method is used to add single key and value pair into the collector
	 * @param key Object
	 * @param value Object
	 */
	public void add(Object key, Object value){
		if(!outputCollector.containsKey(key)){
			ArrayList<Object> tempList = new ArrayList<Object>();
			tempList.add(value);
			outputCollector.put(key, tempList);
		} else {
			outputCollector.get(key).add(value);
		}
	}
}
