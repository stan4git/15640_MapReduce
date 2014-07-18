package examples;

import format.MapperOutputCollector;

import mapred.*;

public class NGramMapper implements Mapper {
	/**
	 * This is 5-Gram
	 */
	@Override
	public void map(Object key, Object value,
			MapperOutputCollector mapperOutputCollector) {
		String line = value.toString().trim().toLowerCase();
		line = line.replaceAll("[^a-z ]", " ");
		line = line.trim().replaceAll(" +", " ");
		String[] split = line.split(" ");

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
				mapperOutputCollector.add(str, 1);
			}
		}
	}
}
