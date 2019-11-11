package edu.mum.cs.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

public class AverageComputation {
	private static Logger logger = Logger.getLogger(WordCount.class);

	public static class MyMap extends Mapper<IntWritable, Text, Text, IntWritable> {
		private Text word = new Text();

		public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			// get ip and last quantity
			String[] strArr = line.split(" ");
			word.set(strArr[0]);
			context.write(word, new IntWritable(Integer.valueOf(strArr[strArr.length-1])));
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Integer sum = 0;
			Integer count = 0;
			for (IntWritable val : values) {
				sum += val.get();
				count++;
			}
			context.write(key, new IntWritable(Integer.valueOf(sum/count)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "AverageComputation");

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
