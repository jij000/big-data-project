package edu.mum.cs.wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.mum.cs.wordcount.MatrixMultiply.MatrixMapper;
import edu.mum.cs.wordcount.MatrixMultiply.MatrixReducer;

public class SparseMatrixMultiply extends Configured implements Tool {
	public static class SMMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String flag = null;
		private int m = 4;//
		private int p = 2;//

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getName();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] val = value.toString().split(",");
			if ("t1".equals(flag)) {
				for (int i = 1; i <= p; i++) {
					context.write(new Text(val[0] + "," + i), new Text("a," + val[1] + "," + val[2]));
				}
			} else if ("t2".equals(flag)) {
				for (int i = 1; i <= m; i++) {
					context.write(new Text(i + "," + val[1]), new Text("b," + val[0] + "," + val[2]));
				}
			}
		}
	}

	public static class SMReducer extends Reducer<Text, Text, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String, String> mapA = new HashMap<String, String>();
			Map<String, String> mapB = new HashMap<String, String>();

			for (Text value : values) {
				String[] val = value.toString().split(",");
				if ("a".equals(val[0])) {
					mapA.put(val[1], val[2]);
				} else if ("b".equals(val[0])) {
					mapB.put(val[1], val[2]);
				}
			}

			int result = 0;

			Iterator<String> mKeys = mapA.keySet().iterator();
			while (mKeys.hasNext()) {
				String mkey = mKeys.next();
				if (mapB.get(mkey) == null) {//
					continue;
				}
				result += Integer.parseInt(mapA.get(mkey)) * Integer.parseInt(mapB.get(mkey));
			}
			context.write(key, new IntWritable(result));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new SparseMatrixMultiply(), args);

		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "WordCount");

		job.setJarByClass(MatrixMultiply.class);

		job.setMapperClass(MatrixMapper.class);
		job.setReducerClass(MatrixReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// Remove output folder
		FileSystem fs = FileSystem.newInstance(getConf());
		Path path = new Path(args[1]);
		fs.delete(path, true);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}