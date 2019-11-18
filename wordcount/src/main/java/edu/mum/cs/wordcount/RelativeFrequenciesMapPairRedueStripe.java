package edu.mum.cs.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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

public class RelativeFrequenciesMapPairRedueStripe {
	private static Logger logger = Logger.getLogger(RelativeFrequenciesMapPairRedueStripe.class);

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
			}
		}
	}

	public static class Reduce extends Reducer<KeyPair, LongWritable, KeyPair, DoubleWritable> {

		private String uPrev = "";
		Double sum = 0.0;
		private Map<String, Long> tempMap;

		public void setup(Context context) throws IOException, InterruptedException {
			this.uPrev = "";
			tempMap = new TreeMap<String, Long>();
		}

		public void reduce(KeyPair key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			// if key.left change, emit the map
			if (!uPrev.equals(key.getItem1()) && !"".equals(uPrev)) {
				for (String key1 : tempMap.keySet()) {
					context.write(new KeyPair(uPrev, key1), new DoubleWritable(tempMap.get(key1) / sum));
					logger.info("key = (" + uPrev + ", " + key1 + ")");
				}
				tempMap = new HashMap<String, Long>();
				sum = 0.0;
			}
			// make a map to store stripe
			for (LongWritable val : values) {
				logger.info("key = " + key);
				sum += Double.valueOf(val.toString());
				if (tempMap.get(key.getItem2()) == null) {
					tempMap.put(key.getItem2(), Long.valueOf(val.toString()));
				} else {
					tempMap.put(key.getItem2(), tempMap.get(key.getItem2()) + Long.valueOf(val.toString()));
				}
			}
			uPrev = key.getItem1();
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// emit the last map
			for (String key1 : tempMap.keySet()) {
				context.write(new KeyPair(uPrev, key1), new DoubleWritable(tempMap.get(key1) / sum));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		long startTime=System.currentTimeMillis(); 
		
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "RelativeFrequenciesMapPairRedueStripe");

		// add
		job.setJarByClass(RelativeFrequenciesMapPairRedueStripe.class);

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
