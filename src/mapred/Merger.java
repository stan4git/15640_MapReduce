/**
 * 
 */
package mapred;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import format.KVPair;

/**
* This class is used to merge all the contents of the partitions from
* different nodes into one
* 
* @author menglonghe
* @author sidilin
*
*/
public class Merger {
	/**
	 * This method is used to merger the contents from 
	 * different partitions into key-pair value
	 * @param paths
	 * @return ArrayList<KVPair>
	 */
	private static ArrayList<KVPair> mergeHelper (HashSet<String> paths) {
		ArrayList<KVPair> res = new ArrayList<KVPair>();
				
		SortedSet<KVPair> sortedRes = new TreeSet<KVPair>(new OrderByKey());
		HashMap<KVPair, Integer> count = new HashMap<KVPair, Integer>();
		
		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
			
		try {
				
			for(String path : paths) {
				
				fis = new FileInputStream(path);
				isr = new InputStreamReader(fis);
				br = new BufferedReader(isr); 
				
				while(true) {
					String curLine = br.readLine();
					if(curLine == null) {
						break;
					}
					String key = curLine.split("\t")[0];
					String value = curLine.split("\t")[1];
					KVPair kv = new KVPair(key, value);
					if(count.containsKey(kv)) {
						count.put(kv, count.get(kv) + 1);
					} else {
						sortedRes.add(kv);
						count.put(kv, 1);
					}
				}
			}
		
			} catch (FileNotFoundException e) {
//				e.printStackTrace();
			} catch (IOException e) {
//				e.printStackTrace();
			} finally {
				try {
					br.close();
					isr.close();
					fis.close();
				} catch (IOException e) {
//					e.printStackTrace();
				}
			}

		System.out.println(count.size());
		Iterator<KVPair> iterator = sortedRes.iterator();
		
		while(iterator.hasNext()) {
			KVPair kvPair = iterator.next();
			
			if(count.get(kvPair) == 1) {
				res.add(kvPair);
			} else {
				for(int i = 0; i < count.get(kvPair); i++) {
					res.add(kvPair);
				}
			}
		}
		
		return res;
		
	}
	
	/**
	 * This method is used to combine the values
	 * @param paths
	 * @return ArrayList<KVPair> 
	 */
	public static ArrayList<KVPair> combineValues (HashSet<String> paths) {
		
		ArrayList<KVPair> res = new ArrayList<KVPair> ();
		ArrayList<KVPair> mergeRes = mergeHelper(paths);
		
		if(mergeRes.size() == 0) {
			return res;
		}
		KVPair first = mergeRes.get(0);
		String lastKey = (String) first.getKey();
		String lastValue = (String) first.getValue();
		ArrayList<String> values = new ArrayList<String>();
		values.add(lastValue);
		
		for(int i = 1; i < mergeRes.size(); i++) {
			KVPair kvPair = mergeRes.get(i);
			String key = (String) kvPair.getKey();
			String value = (String) kvPair.getValue();
			if(!key.equals(lastKey)) {
				res.add(new KVPair(lastKey, values));
				lastKey = key;
				lastValue = value;
				values = new ArrayList<String>();
			} 
			values.add(value);
		}
		
		if(values.size() != 0) {
			res.add(new KVPair(lastKey, values));
		}
		
		return res;
	}
	
	private static class OrderByKey implements Comparator<KVPair> {

		@Override
		public int compare(KVPair k1, KVPair k2) {
			if(((String) k1.getKey()).compareTo((String)k2.getKey()) == 0) {
				return ((String) k1.getValue()).compareTo((String)k2.getValue());
			}
			return ((String) k1.getKey()).compareTo((String)k2.getKey());
		}
	}

}
