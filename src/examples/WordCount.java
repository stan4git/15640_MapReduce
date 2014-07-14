package examples;

import java.io.IOException;
import format.LineFormat;
import format.OutputFormat;
import mapred.JobClient;
import mapred.JobConfiguration;

public class WordCount {

	public static void main(String[] args) {
		JobConfiguration conf = new JobConfiguration();
		conf.setInputfile("input.txt");
		conf.setOutputfile("output.txt");
		conf.setInputFormat(LineFormat.class);
		conf.setOutputFormat(OutputFormat.class);
		conf.setMapperClass(WordCountMapper.class);
		conf.setReducerClass(WordCountReducer.class);

		JobClient jobclient = new JobClient();

		try {
			jobclient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
