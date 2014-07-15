package examples;

import java.util.ArrayList;

import mapred.Reducer;
import format.ReducerOutputCollector;

public class NGramReducer implements Reducer {
	private int result = 0;

	@Override
	public void reduce(String key, ArrayList<String> values,
			ReducerOutputCollector outputCollector) {
		int sum = 0;
		for (String value : values) {
			sum += Integer.parseInt(value);
		}
		result = sum;
		outputCollector.add(key, result);
	}
}
