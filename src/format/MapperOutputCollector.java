package format;

import java.util.ArrayList;

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
public class MapperOutputCollector {
	
	// such as <Hello,<1,1,1,1,1>>
	public ArrayList<KVPair> mapperOutputCollector = new ArrayList<KVPair>();

	/**
	 * This method is used to add single key and value pair into the collector
	 * @param key Object
	 * @param value Object
	 */
	public void add(Object key, Object value){
		KVPair kvPair = new KVPair(key,value);
		mapperOutputCollector.add(kvPair);
	}
}
