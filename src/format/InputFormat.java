package format;

import java.util.ArrayList;
import java.util.List;

/**
 * This is an abstract class which contains the properties: inputContents,
 * kvPairs. It also defines the getKvPairs method
 * 
 * @author menglonghe
 * 
 */
public abstract class InputFormat {

	public String inputContents;

	public List<KVPair> kvPairs = new ArrayList<KVPair>();

	public abstract List<KVPair> getKvPairs();
}
