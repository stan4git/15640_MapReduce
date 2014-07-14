package examples;

import java.util.ArrayList;

import format.ReducerOutputCollector;
import mapred.Reducer;

public class WordCountReducer implements Reducer {

	@Override
	public void reduce(String key, ArrayList<String> value,
			ReducerOutputCollector outputCollector) {
		outputCollector.add(key, value.size());
	}

}
