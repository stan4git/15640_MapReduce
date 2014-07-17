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
				if (n + start == split.length + 1) {
					break;
				}

				StringBuilder sb = new StringBuilder();
				for (int count = 0; count < n; count++) {
					sb.append(split[start + count]);
					if (count != n - 1) {
						sb.append(" ");
					}
				}

				String str = sb.toString();
				mapperOutputCollector.add(str, 1);
			}
		}
	}
}
