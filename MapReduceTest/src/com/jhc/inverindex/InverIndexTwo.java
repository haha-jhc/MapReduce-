package com.jhc.inverindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.jhc.inverindex.InverIndexStepOne.InverIndexStepOneMapper;
import com.jhc.inverindex.InverIndexStepOne.InverIndexStepOneReducer;

public class InverIndexTwo {
	
	static class InverIndexTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String[] word_file= line.split("--");
			context.write(new Text(word_file[0]), new Text(word_file[1]));
			
		}
	}
	
	static class InverIndexStepTwoReducer extends Reducer<Text, Text, Text,Text >{
		
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text value:values){
				sb.append(value.toString());
			}
			context.write(key, new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		
		Job job=Job.getInstance(conf);
		job.setJarByClass(InverIndexTwo.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://wordcount/inverindex_stepone"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://wordcount/inverindex_steptwo"));
		
		job.setMapperClass(InverIndexTwoMapper.class);
		job.setReducerClass(InverIndexStepTwoReducer.class);
		
		job.waitForCompletion(true);
		
	}

}
