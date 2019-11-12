package edu.mum.cs.wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

import edu.mum.cs.wordcount.util.MyPair;

public class InMapperAverageComputation {
	private static Logger logger = Logger.getLogger(WordCount.class);
	
	public static class MyMap extends Mapper<LongWritable, Text, Text, MyPair> {
		private Text word = new Text();
		private Map<String, MyPair> mapCount;
		
		public void setup(Context context) throws IOException, InterruptedException {
			mapCount = new HashMap<String, MyPair>();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			// get ip and last quantity
			String[] strArr = line.split(" ");
			logger.info("@@@@ " + strArr[0] + " ### " + strArr[strArr.length - 1]);
			// when the last quantity is - then 0
			Long val = "-".equals(strArr[strArr.length - 1]) ? 0L : Long.valueOf(strArr[strArr.length - 1]);
			String keyStr = strArr[0];
			if (mapCount.get(keyStr) == null) {
				MyPair myPair = new MyPair(val, 1L);
				mapCount.put(keyStr, myPair);
			} else {
				mapCount.get(keyStr).setVal(mapCount.get(keyStr).getVal() + val);
				mapCount.get(keyStr).setCnt(mapCount.get(keyStr).getCnt() + 1L);
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {

			for (String key : this.mapCount.keySet()) {
				word.set(key);
				logger.info("cleanup my log: " + word + ";" + mapCount.get(key).getVal() + ";" + mapCount.get(key).getCnt());
				context.write(word, mapCount.get(key));
			}
		}
	}

	public static class Reduce extends Reducer<Text, MyPair, Text, LongWritable> {

		public void reduce(Text key, Iterable<MyPair> values, Context context)
				throws IOException, InterruptedException {
			Long sum = 0L;
			Long count = 0L;
			for (MyPair val : values) {
				sum += val.getVal();
				count += val.getCnt();
			}
			context.write(key, new LongWritable(Long.valueOf(sum/count)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "AverageComputation");

		// add
		job.setJarByClass(WordCount.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MyPair.class);
		job.setOutputKeyClass(Text.class);
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
