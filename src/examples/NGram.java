package examples;
import java.io.IOException;

import format.LineFormat;
import format.OutputFormat;
import mapred.*;

public class NGram {
	public static void main(String[] args) {
		JobConfiguration conf = new JobConfiguration();
		conf.setInputfile("pg_01");
		conf.setOutputfile("NGramOutput");
		conf.setInputFormat(LineFormat.class);
		conf.setOutputFormat(OutputFormat.class);
		conf.setMapperClass(NGramMapper.class);
		conf.setReducerClass(NGramReducer.class);

		JobClient jobclient = new JobClient();

		try {
			jobclient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

