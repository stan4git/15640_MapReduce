package mapred;

import format.MapperOutputCollector;

/**
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public interface Mapper {
	public void map(Object key, Object value, MapperOutputCollector mapperOutputCollector);
}
