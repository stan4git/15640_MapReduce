package format;

import java.util.List;

/**
 * This class extends the InputFormat class. Format the input contents into the
 * Key-Value pairs. The target of this class is to the "key value /n" format
 * file, such as the Mapper's output
 * 
 * @author menglonghe
 * @author sidilin
 * 
 */
public class PairFormat extends InputFormat {

	/**
	 * The default constructor
	 * 
	 * @param inputContents
	 *            String
	 */
	public PairFormat(String inputContents) {
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
			String[] words = lines[i].trim().split(" ");
			kvPairs.add(new KVPair(words[0].trim(), words[1].trim()));
		}
		return kvPairs;
	}
}
