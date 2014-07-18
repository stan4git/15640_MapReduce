package examples;

import java.util.TreeMap;
import java.util.Map.Entry;

import format.MapperOutputCollector;

import mapred.*;

public class NGramMapper implements Mapper {
	private String word = new String();
	private int times = 0;
	
	@Override
	public void map(Object key, Object value,
			MapperOutputCollector mapperOutputCollector) {
		String line = value.toString().trim().toLowerCase();
		line = line.replaceAll("[^a-z ]", " ");
		line = line.trim().replaceAll(" +", " ");
		String[] split = line.split(" ");
		TreeMap<String, Integer> combine = new TreeMap<String, Integer>();
		for (int start = 0; start < split.length; start++) {
			for (int n = 1; n <= 5; n++) {
				if (n + start > split.length) {
					break;
				}
				
				StringBuilder sb = new StringBuilder();
				for (int index = 0; index < n; index++) {
					sb.append(split[start + index]);
					if (index < n - 1) {
						sb.append(" ");
					}
				}
				
				String str = sb.toString();
				if (combine.containsKey(str)) {
					combine.put(str, combine.get(str) + 1);
				} else {
					combine.put(str, 1);
				}
			}
		}
		
		while (combine.size() != 0) {
			Entry<String, Integer> temp = combine.pollFirstEntry();
			word = temp.getKey();
			times = temp.getValue();
			mapperOutputCollector.add(word, times);
		}
	}
}
