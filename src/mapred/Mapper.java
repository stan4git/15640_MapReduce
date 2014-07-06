package mapred;

import format.OutputCollector;

/**
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public interface Mapper {
	public void map(Object key, Object value, OutputCollector outputCollector);
}
