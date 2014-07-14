package examples;

import format.MapperOutputCollector;
import mapred.Mapper;

public class WordCountMapper implements Mapper{

	@Override
	public void map(Object key, Object value,
			MapperOutputCollector mapperOutputCollector) {
		String newKey = (String)value;
		int newValue = 1;
		mapperOutputCollector.add(newKey, newValue);
	}

}
