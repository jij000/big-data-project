package edu.mum.cs.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import edu.mum.cs.wordcount.util.KeyPair;


public class RelativeFrequenciesPair {
	private static Logger logger = Logger.getLogger(RelativeFrequenciesPair.class);

	public static class MyMap extends Mapper<LongWritable, Text, KeyPair, LongWritable> {
//		private Text word = new Text();
		private final static LongWritable ONE = new LongWritable(1L);

		// get window keyPair list
		public List<KeyPair> window(String line) {
			List<KeyPair> windowList = new ArrayList<KeyPair>();
			String[] strArr = line.split(" ");
			for (int i = 0; i < strArr.length - 1; i++) {
				for (int j = i + 1; j < strArr.length; j++) {
					if (!strArr[i].equals(strArr[j])) {
						KeyPair key = new KeyPair(strArr[i], strArr[j]);
						logger.info("key = " + strArr[i] + " , " + strArr[j]);
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
			// get window keyPair list
			List<KeyPair> keyList = window(line);
			for (KeyPair keyPair : keyList) {
				context.write(keyPair, ONE);
				KeyPair keyStar = new KeyPair(keyPair.getItem1(), "*");
				context.write(keyStar, ONE);
			}
		}
	}

	public static class Reduce extends Reducer<KeyPair, LongWritable, KeyPair, DoubleWritable> {
		private Double sumAll = 0.0;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			sumAll = 0.0;
		}

		public void reduce(KeyPair key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			Double sum = 0.0;
			for (LongWritable val : values) {
				sum += Double.valueOf(val.get());
			}
			if ("*".equals(key.getItem2())) {
				sumAll = sum;
			} else {
				context.write(key, new DoubleWritable(sum / sumAll));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		long startTime=System.currentTimeMillis(); 
		
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "RelativeFrequenciesPair");

		// add
		job.setJarByClass(WordCount.class);

		job.setOutputKeyClass(KeyPair.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(MyMap.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		long endTime=System.currentTimeMillis();
		logger.info("Time used:" +(endTime-startTime)+"ms");
	}
}
