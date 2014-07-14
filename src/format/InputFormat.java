package format;

import java.util.ArrayList;
import java.util.List;

/**
 * This is an abstract class which contains the properties: inputContents,
 * kvPairs. It also defines the getKvPairs method
 * 
 * @author menglonghe
 * @author sidilin
 * 
 */
public abstract class InputFormat {
	/**
	 *  the input contents as the String format
	 */
	public String inputContents;
	/**
	 * The arrayList to store the KV pairs
	 */
	public List<KVPair> kvPairs = new ArrayList<KVPair>();
	/**
	 * This method is used to wrap the content into KV pairs
	 * and store into the List
	 * @return
	 */
	public abstract List<KVPair> getKvPairs();
}
