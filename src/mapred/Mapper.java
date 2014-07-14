package mapred;

import format.MapperOutputCollector;

/**
 * This class is a interface of programmer's Mapper
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public interface Mapper {
	/**
	 * This method is used to collect the data from the key and value and put 
	 * them all into the MapperOutputCollector
	 * 
	 * @param key
	 * @param value
	 * @param mapperOutputCollector
	 */
	public void map(Object key, Object value, MapperOutputCollector mapperOutputCollector);
}
