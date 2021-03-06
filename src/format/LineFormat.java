package format;

import java.util.List;

/**
 * This class is used to format the input String. Format the input contents into
 * the Key-Value pairs. Each Key presents the line Number and value represents 
 * the whole line. The target of this class is to the original .txt or
 * other raw materials.
 * 
 * @author menglonghe
 * @author sidilin
 * 
 */
public class LineFormat extends InputFormat {

	/**
	 * The default constructor
	 * 
	 * @param inputContents
	 *            String the input raw materials
	 */
	public LineFormat(String inputContents) {
		this.inputContents = inputContents;
	}

	/**
	 * This method is used to format the input contents into the Key-Value pairs
	 * and return an Arraylist
	 */
	@Override
	public List<KVPair> getKvPairs() {
		String[] lines = inputContents.split("\n");
		for (int i = 0; i < lines.length; i++) {
			kvPairs.add(new KVPair(Integer.toString(i),lines[i].trim()));
		}
		return kvPairs;
	}
}
