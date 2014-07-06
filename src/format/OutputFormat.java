package format;

import java.util.ArrayList;
import java.util.SortedMap;

/**
 * This class is used to format the OutputCollector object to a string. This
 * string has a format "key value \n"
 * 
 * @author menglonghe
 * @author sidilin
 * 
 */
public class OutputFormat {

	/**
	 * This method is actually to format the OutputCollector object to a
	 * string.This string has a format "key value \n"
	 * 
	 * @param collector
	 *            this is a OutputCollector object
	 * @return String
	 */
	public String formatOutput(ReducerOutputCollector collector) {
		StringBuffer sb = new StringBuffer("");
		SortedMap<Object, ArrayList<Object>> outputMap = collector.outputCollector;
		for (Object key : outputMap.keySet()) {
			for (Object value : outputMap.get(key)) {
				sb.append(key);
				sb.append(" ");
				sb.append(value);
				sb.append("\n");
			}
		}
		return sb.toString();
	}
}
