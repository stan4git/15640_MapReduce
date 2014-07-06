package mapred;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.SortedMap;

/**
 * 1. take buffer as input, partition data into different partition (partition
 * path) 2. get hashcode of each key
 */
public class Partitioner {

	public static StringBuffer[] partition(
			SortedMap<Object, ArrayList<Object>> collection,
			Integer partitionNum) {

		HashMap<Object, Object> hashcodes = new HashMap<Object, Object>();
		StringBuffer[] partitions = new StringBuffer[partitionNum];

		System.out.println("uniqueKeys: " + collection.size());

		for (int i = 0; i < partitionNum; i++){
			partitions[i] = new StringBuffer("");
		}

		for (Object key : collection.keySet()) {
			hashcodes.put(key, (key.toString().hashCode()) % partitionNum);
		}

		for (Object key : collection.keySet()) {
			int index = (int) hashcodes.get(key);
			for (Object value : collection.get(key)) {
				partitions[index].append(key);
				partitions[index].append(" ");
				partitions[index].append(value);
				partitions[index].append("\n");
			}
		}
		return partitions;
	}
}
