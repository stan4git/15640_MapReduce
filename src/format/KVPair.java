package format;

import java.io.Serializable;

/**
 * 
 * This class defines a structure to store the key and value pair
 * 
 * @author menglonghe
 * @author sidilin
 *
 */
public class KVPair implements Serializable{

	private static final long serialVersionUID = -104757529176551147L;
	
	public Object key;
	public Object value;
	
	/**
	 * The default constructor
	 * @param key
	 * @param value
	 */
	public KVPair(Object key, Object value) {
		this.key = key;
		this.value = value;
	}
	
	/**
	 * Override the equals methods
	 */
	@Override
	public boolean equals(Object object) {
		KVPair kvPair = (KVPair)object;
		return key.equals(kvPair.key) && value.equals(kvPair.value);
	}

}
