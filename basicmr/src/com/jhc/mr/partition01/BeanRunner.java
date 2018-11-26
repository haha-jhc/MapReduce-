package com.jhc.mr.partition01;

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

public class BeanRunner {

	static class BeanMapper extends Mapper<LongWritable, Text, Text, Bean>{
		Bean tx =new Bean();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Bean>.Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String[] fields= StringUtils.split(line,"\t");
			String timestamp=fields[0];
			String ip=fields[1];
			Text domain =new Text(fields[4]);
			tx.set(timestamp, ip);
			context.write(domain, tx);
		}
	}
	
	static class BeanReducer extends Reducer<Text, Bean, Text, Bean>{
		@Override
		protected void reduce(Text key, Iterable<Bean> values, Reducer<Text, Bean, Text, Bean>.Context context)
				throws IOException, InterruptedException {
			
			for(Bean bean:values){
				context.write(key, bean);
			}
			
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(BeanRunner.class);
		
		job.setMapperClass(BeanMapper.class);
		job.setReducerClass(BeanReducer.class);
		
		//指定自定义的partitioner类，替换默认的HashPartitioner
		job.setPartitionerClass(ProvincePartitioner.class);
		
		//指定reduce task 数量 
		job.setNumReduceTasks(6);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Bean.class);
		
		String inputPath ="/usr/jhc/mapreduce/data";
		String outputPath = "/usr/jhc/mapreduce/Partitioner";
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
