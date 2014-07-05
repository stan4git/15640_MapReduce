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
	
	private Object key;
	private Object value;
	
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

	public Object getKey() {
		return key;
	}

	public void setKey(Object key) {
		this.key = key;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}
	
	

}
