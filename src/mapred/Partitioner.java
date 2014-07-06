package mapred;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.SortedMap;

import format.KVPair;

/**
 * 1. take buffer as input, partition data into different partition (partition
 * path) 2. get hashcode of each key
 */
public class Partitioner {

	public static StringBuffer[] partition(
			ArrayList<KVPair> collection,
			Integer partitionNum) {

		HashMap<Object, Object> hashcodes = new HashMap<Object, Object>();
		StringBuffer[] partitions = new StringBuffer[partitionNum];

		System.out.println("uniqueKeys: " + collection.size());

		for (int i = 0; i < partitionNum; i++){
			partitions[i] = new StringBuffer("");
		}

		for (KVPair kvPair : collection) {
			hashcodes.put(kvPair.getKey(), (kvPair.getValue().toString().hashCode()) % partitionNum);
		}

		for (KVPair kvPair : collection) {
			int index = (int) hashcodes.get(kvPair.getKey());
			partitions[index].append(kvPair.getKey());
			partitions[index].append(" ");
			partitions[index].append(kvPair.getValue());
			partitions[index].append("\n");
		}
		return partitions;
	}
}
