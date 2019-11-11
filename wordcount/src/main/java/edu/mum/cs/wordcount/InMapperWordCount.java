package edu.mum.cs.wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class InMapperWordCount {
	private static Logger logger = Logger.getLogger(WordCount.class);

	public static class MyMap extends Mapper<LongWritable, Text, Text, IntWritable> {
//		private final static IntWritable one = new IntWritable(1);
		int inMapperCount = 0;
		private Text word = new Text();
		private Map<String, Integer> mapCount;

		public void setup(Context context) throws IOException, InterruptedException {
			mapCount = new HashMap<String, Integer>();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String keyStr = tokenizer.nextToken();
				logger.info("my log: " + word);
				if (mapCount.get(keyStr) == null) {
					mapCount.put(keyStr, 1);
				} else {
					int temp = mapCount.get(keyStr) + 1;
					mapCount.put(keyStr, temp);
				}
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {

			for (String key : this.mapCount.keySet()) {
				word.set(key);
				logger.info("cleanup my log: " + word);
				context.write(word, new IntWritable(mapCount.get(key)));
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "InMapperWordCount");

		// add
		job.setJarByClass(WordCount.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MyMap.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
