package util;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class FunctionalUtil {
	
	public static <K1, K2, V> ConcurrentHashMap<K1, Hashtable<K2, HashSet<V>>> deepCopy(
			ConcurrentHashMap<K1, Hashtable<K2, HashSet<V>>> original) {
		ConcurrentHashMap<K1, Hashtable<K2, HashSet<V>>> copy = new ConcurrentHashMap<K1, Hashtable<K2, HashSet<V>>>();
		for (Entry<K1, Hashtable<K2, HashSet<V>>> entry : original.entrySet()) {
			Hashtable<K2, HashSet<V>> deepCopyValue = new Hashtable<K2, HashSet<V>>();
			Hashtable<K2, HashSet<V>> oriValues = entry.getValue();
			for (Entry<K2, HashSet<V>> entrySecond : oriValues.entrySet()) {
				HashSet<V> setSecondLoop = entrySecond.getValue();
				HashSet<V> newSet = deepCopyHashSet(setSecondLoop);
				deepCopyValue.put(entrySecond.getKey(), newSet);
			}
			copy.put(entry.getKey(), deepCopyValue);
		}
		return copy;
	}

	public static <V> HashSet<V> deepCopyHashSet(HashSet<V> ori) {
		HashSet<V> copySet = new HashSet<V>();
		for (V v : ori) {
			copySet.add(v);
		}
		return copySet;
	}
}
