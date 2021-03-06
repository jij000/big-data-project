package edu.mum.cs.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import edu.mum.cs.wordcount.util.PredictPair;


public class AbsoluteFrequencies {
	private static Logger logger = Logger.getLogger(WordCount.class);

	public static class MyMap extends Mapper<LongWritable, Text, PredictPair, LongWritable> {
//		private Text word = new Text();
		private final static LongWritable one = new LongWritable(1L);

		// get window predictPair list
		public List<PredictPair> window(String customerId, String line) {
			List<PredictPair> windowList = new ArrayList<PredictPair>();
			String[] strArr = line.split(" ");
			for (int i = 0; i < strArr.length - 1; i++) {
				for (int j = i + 1; j < strArr.length; j++) {
					if (!strArr[i].equals(strArr[j])) {
						PredictPair key = new PredictPair(customerId, strArr[i], strArr[j]);
						logger.info("key = " + customerId + " , " + strArr[i] + " , " + strArr[j]);
						windowList.add(key);
					} else {
						break;
					}
				}
			}
			return windowList;
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			// get window predictPair list
			List<PredictPair> keyList = window(key.toString(), line);
			for (PredictPair predictPair : keyList) {
				context.write(predictPair, one);
			}
		}
	}

	public static class Reduce extends Reducer<PredictPair, LongWritable, PredictPair, LongWritable> {

		public void reduce(PredictPair key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			Long sum = 0L;
			for (LongWritable val : values) {
				sum += val.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "AbsoluteFrequencies");

		// add
		job.setJarByClass(WordCount.class);

		job.setOutputKeyClass(PredictPair.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(MyMap.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
