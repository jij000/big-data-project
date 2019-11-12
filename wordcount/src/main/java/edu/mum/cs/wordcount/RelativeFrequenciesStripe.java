package edu.mum.cs.wordcount;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import edu.mum.cs.wordcount.util.KeyPair;

public class RelativeFrequenciesStripe {
	private static Logger logger = Logger.getLogger(RelativeFrequenciesStripe.class);

	public static class MyMap extends Mapper<LongWritable, Text, Text, MapWritable> {
//		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			// get window keyPair list
			String[] strArr = line.split(" ");
			for (int i = 0; i < strArr.length - 1; i++) {
				Map<String, Long> tempMap = new TreeMap<String, Long>();
				for (int j = i + 1; j < strArr.length; j++) {
					if (!strArr[i].equals(strArr[j])) {
						if (tempMap.get(strArr[j]) == null) {
							tempMap.put(strArr[j], 1L);
						} else {
							tempMap.put(strArr[j], tempMap.get(strArr[j]) +  1L);
						}
						logger.info("key = " + strArr[i] + " , " + tempMap.get(strArr[i]));
					} else {
						break;
					}
				}
				// copy MapWritable and emit
				MapWritable tempMapWritable = new MapWritable();
				for (String key1:tempMap.keySet()) {
					tempMapWritable.put(new Text(key1), new LongWritable(tempMap.get(key1)));
				}
				context.write(new Text(strArr[i]), tempMapWritable);
			}
		}
	}

	public static class Reduce extends Reducer<Text, MapWritable, KeyPair, DoubleWritable> {
	
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			Map<String, Long> finalMap = new TreeMap<String, Long>();
			Double sum = 0.0;
			for (MapWritable val : values) {
				logger.info("key = " + key + " , " + val.keySet().size());
				for (Writable tkey : val.keySet()) {
					if (finalMap.get(tkey.toString())==null) {
						finalMap.put(tkey.toString(), Long.valueOf(val.get(tkey).toString()));
					} else {
						finalMap.put(tkey.toString(), finalMap.get(tkey.toString()) + Long.valueOf(val.get(tkey).toString()));
					}
					sum += Double.valueOf(val.get(tkey).toString());
				}
			}
			for (String key1 : finalMap.keySet()) {
				context.write(new KeyPair(key.toString(), key1), new DoubleWritable( finalMap.get(key1)/ sum));
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "RelativeFrequenciesStripe");

		// add
		job.setJarByClass(RelativeFrequenciesStripe.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		job.setMapperClass(MyMap.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
