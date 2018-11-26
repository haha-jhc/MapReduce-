package com.jhc.basicmr;


import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountRunner {

	static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		int count=0;
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line,"\t");
			Text domain = new Text(fields[4]);
			context.write(new Text(domain), new LongWritable(1));
		}
	}
	
	static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			
			long counter=0;
			
			for(LongWritable value:values){
				//累加每一个value
				counter +=value.get();
			}
			context.write(key, new LongWritable(counter));			
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		FileSystem fs= FileSystem.get(conf);
		Job job=Job.getInstance(conf);
		//设置本次job作业所在的jar包
		job.setJarByClass(WordCountRunner.class);
		//指明本次作业使用的Mapper类是哪个
		job.setMapperClass(WordCountMapper.class);
		//指明本次作业使用的Reduce类是哪个
		job.setReducerClass(WordCountReducer.class);
		//指明Mapper类的输出数据key的类型
		job.setMapOutputKeyClass(Text.class);
		//指明Mapper类的输出数据value的类型
		job.setMapOutputValueClass(LongWritable.class);
		//指明Reduce类的输出数据key的类型
		job.setOutputKeyClass(Text.class);
		//指明Reduce类的输出数据value的类型
		job.setOutputValueClass(LongWritable.class);
		
		job.setNumReduceTasks(1); //设置reduce的个数
		
		String inputPath = "/usr/jhc/mapreduce/data";
		String outputPath = "/usr/jhc/mapreduce/wordcount";
		
		//判断output 文件夹是否存在
		Path path=new Path(outputPath);
		FileSystem fileSystem =path.getFileSystem(conf);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path,true);
		}
		
		// //本次job作业要处理的原始数据所在路径
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		// //本次job作业产生结果的输出路径
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		//提交本次作业
		job.waitForCompletion(true);
		
	}

}
