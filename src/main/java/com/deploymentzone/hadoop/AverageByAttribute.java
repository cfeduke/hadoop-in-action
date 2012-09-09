package com.deploymentzone.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.thirdparty.guava.common.base.Splitter;
import org.apache.hadoop.thirdparty.guava.common.collect.Iterables;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

public class AverageByAttribute extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable,Text,Text,Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String fields[] = value.toString().split(",", -20);
			String country = fields[4];
			String numClaims = fields[8];
			if (numClaims.length() > 0 && !numClaims.startsWith("\"")) {
				context.write(new Text(country), new Text(numClaims + ",1"));
			}
		}

	}

	public static class Reduce extends Reducer<Text,Text,Text,DoubleWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (Text value : values) {
				String fields[] = value.toString().split(",");
				sum += Double.parseDouble(fields[0]);
				count += Integer.parseInt(fields[1]);
			}
			context.write(key, new DoubleWritable(sum/count));
		}

	}

	public static class Combine extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (Text value : values) {
				String fields[] = value.toString().split(",");
				sum += Double.parseDouble(fields[0]);
				count += Integer.parseInt(fields[1]);
			}
			context.write(key, new Text(sum + "," + count));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job();

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    int result = ToolRunner.run(new AverageByAttribute(), otherArgs);
	    System.exit(result);
	 }

}
