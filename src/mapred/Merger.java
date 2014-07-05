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
import format.OutputCollector;

/**
* 
* @author menglonghe
* @author sidilin
*
*/
public class Merger {
	
	public static String merge (HashSet<String> paths) {
		
		StringBuffer res = new StringBuffer();
		
		SortedSet<KVPair> sortedRes = new TreeSet<KVPair>(new OrderByKey());
		HashMap<KVPair, Integer> count = new HashMap<KVPair, Integer>();
		
		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		
		for(String path : paths) {
			
			try {
				
				fis = new FileInputStream(path);
				isr = new InputStreamReader(fis);
				br = new BufferedReader(isr); 
				
				while(true) {
					String curLine = br.readLine();
					if(curLine == null) {
						break;
					}
					String key = curLine.split(" ")[0];
					String value = curLine.split(" ")[1];
					KVPair kv = new KVPair(key, value);
					if(sortedRes.contains(kv)) {
						count.put(kv, count.get(kv) + 1);
					} else {
						sortedRes.add(kv);
						count.put(kv, 1);
					}
					
				}
		
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					br.close();
					isr.close();
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		Iterator<KVPair> iterator = sortedRes.iterator();
		
		while(iterator.hasNext()) {
			KVPair kvPair = iterator.next();
			String key = (String)kvPair.getKey();
			String value = (String)kvPair.getValue();
			
			if(count.get(kvPair) == 1) {
				res.append(key + " " + value + " " + "\n");
			} else {
				for(int i = 0 ; i < count.get(kvPair); i++) {
					res.append(key + " " + value + " " + "\n");
				}
			}
		}
		
		return res.toString();
	}
	
	
	public static ArrayList<KVPair> combineValues (HashSet<String> paths) {
		
		ArrayList<KVPair> res = new ArrayList<KVPair> ();
		
		SortedSet<KVPair> sortedRes = new TreeSet<KVPair>(new OrderByKey());
		HashMap<KVPair, Integer> count = new HashMap<KVPair, Integer>();
		
		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		
		for(String path : paths) {
			
			try {
				
				fis = new FileInputStream(path);
				isr = new InputStreamReader(fis);
				br = new BufferedReader(isr); 
				
				while(true) {
					String curLine = br.readLine();
					if(curLine == null) {
						break;
					}
					String key = curLine.split(" ")[0];
					String value = curLine.split(" ")[1];
					KVPair kv = new KVPair(key, value);
					if(sortedRes.contains(kv)) {
						count.put(kv, count.get(kv) + 1);
					} else {
						sortedRes.add(kv);
						count.put(kv, 1);
					}
					
				}
		
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					br.close();
					isr.close();
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		Iterator<KVPair> iterator = sortedRes.iterator();
		KVPair first = iterator.next();
		if(first == null) {
			return null;
		}
		String lastKey = (String) first.getKey();
		String lastValue = (String) first.getValue();
		ArrayList<String> values = new ArrayList<String>();
		values.add(lastValue);
		
		while(iterator.hasNext()) {
			KVPair kvPair = iterator.next();
			String key = (String) kvPair.getKey();
			String value = (String) kvPair.getValue();
			if(!key.equals(lastKey)) {
				res.add(new KVPair(lastKey, values));
				lastKey = key;
				lastValue = value;
				values = new ArrayList<String>();
			} 
			if(count.get(kvPair) == 1) {
				values.add(value);
			} else {
				for(int i = 0 ; i < count.get(kvPair); i++) {
					values.add(value);
				}
			}
		}
		
		if(values.size() != 0) {
			res.add(new KVPair(lastKey, values));
		}
		
		return res;
	}
	
	private static class OrderByKey implements Comparator<KVPair> {

		@Override
		public int compare(KVPair k1, KVPair k2) {
			return ((String) k1.getKey()).compareTo((String)k2.getKey());
		}
	}

}
