package com.deploymentzone.hadoop;

import java.io.IOException;
import java.util.Iterator;

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

public class CitedCount extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Iterable<String> split = Splitter.on(',').limit(2).split(value.toString());

			context.write(new Text(Iterables.getLast(split)), one);
		}

	}

	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			Iterator<IntWritable> iter = values.iterator();
			while (iter.hasNext()) {
				iter.next();
				count++;
			}
			context.write(key, new IntWritable(count));
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job();

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
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
	    int result = ToolRunner.run(new CitedCount(), otherArgs);
	    System.exit(result);
	 }

}
