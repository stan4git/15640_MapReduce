package examples;

import java.util.ArrayList;

import mapred.Reducer;
import format.ReducerOutputCollector;

public class NGramReducer implements Reducer {

	@Override
	public void reduce(String key, ArrayList<String> values,
			ReducerOutputCollector outputCollector) {
		int sum = 0;
		for (String value : values) {
			sum += Integer.parseInt(value);
		}
		outputCollector.add(key, sum);
	}
}
