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

public class MatrixMultiply extends Configured implements Tool {
	public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String flag = null;//
		private int rowNum = 4;//
		private int colNum = 2;//
		private int rowIndexA = 1; //
		private int rowIndexB = 1; //

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			flag = ((FileSplit) context.getInputSplit()).getPath().getName();//
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if ("ma".equals(flag)) {
				for (int i = 1; i <= colNum; i++) { // column first
					Text k = new Text(rowIndexA + "," + i);
					for (int j = 0; j < tokens.length; j++) {
						Text v = new Text("a," + (j + 1) + "," + tokens[j]);
						context.write(k, v);
					}
				}
				rowIndexA++;//
			} else if ("mb".equals(flag)) {
				for (int i = 1; i <= rowNum; i++) {
					for (int j = 0; j < tokens.length; j++) {
						Text k = new Text(i + "," + (j + 1));
						Text v = new Text("b," + rowIndexB + "," + tokens[j]);
						context.write(k, v);
					}
				}
				rowIndexB++;//
			}
		}
	}

	public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {
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

//        Configuration conf = new Configuration();
//        conf.addResource("classpath:/hadoop/core-site.xml");
//        conf.addResource("classpath:/hadoop/hdfs-site.xml");
//        conf.addResource("classpath:/hadoop/mapred-site.xml");
//        conf.addResource("classpath:/hadoop/yarn-site.xml");

		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new MatrixMultiply(), args);

		System.exit(res);
	}

	public static void main_pre(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String input1 = "hdfs://192.168.1.128:9000/user/lxh/matrix/ma";
		String input2 = "hdfs://192.168.1.128:9000/user/lxh/matrix/mb";
		String output = "hdfs://192.168.1.128:9000/user/lxh/matrix/out";

		Configuration conf = new Configuration();
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		conf.addResource("classpath:/hadoop/yarn-site.xml");

		Job job = Job.getInstance(conf, "MatrixMultiply");
		job.setJarByClass(MatrixMultiply.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MatrixMapper.class);
		// job.setReducerClass(MatrixReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));//
		Path outputPath = new Path(output);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
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