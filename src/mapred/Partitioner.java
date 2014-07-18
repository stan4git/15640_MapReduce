package mapred;

import java.util.ArrayList;
import java.util.HashMap;

import format.KVPair;

/**
 * 1. take buffer as input, partition data into different partition (partition
 * path) 2. get hashcode of each key
 */
public class Partitioner {
	/**
	 * This method is used to partition the contents into the 
	 * specific numbers
	 * 
	 * @param collection the contents which was used to partition
	 * @param partitionNum the specific partition number
	 * @return StringBuffer array to store the partitions
	 */
	public static StringBuffer[] partition(ArrayList<KVPair> collection,
			Integer partitionNum) {

		HashMap<Object, Object> hashcodes = new HashMap<Object, Object>();
		StringBuffer[] partitions = new StringBuffer[partitionNum];

		System.out.println("uniqueKeys: " + collection.size());

		for (int i = 0; i < partitionNum; i++) {
			partitions[i] = new StringBuffer("");
		}

		for (KVPair kvPair : collection) {
			hashcodes.put(kvPair.getKey(),
					Math.abs((kvPair.getKey().toString().hashCode()) % partitionNum));
		}

		for (KVPair kvPair : collection) {
			int index = (int) hashcodes.get(kvPair.getKey());
			partitions[index].append(kvPair.getKey());
			partitions[index].append("\t");
			partitions[index].append(kvPair.getValue());
			partitions[index].append("\n");
		}
		return partitions;
	}
}
