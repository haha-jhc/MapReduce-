package com.jhc.mr.solrkey;

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

public class BeanRunnerSolrKey {

	static class BeanMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			 String line =value.toString();
			 String[] fields = StringUtils.split(line,"\t");
			 String timestamp = fields[0];
			 String ip = fields[1];
			 String domain = fields[4];
			 Text Width =new Text(fields[5]);
			 Text tx = new Text(timestamp +"\t"+domain+"\t"+ip);
			 
			 context.write(Width, tx);
		}
	}
	
	static class BeanReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for(Text value:values){
				context.write(key, value);
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf =new Configuration();
		Job job =Job.getInstance(conf);
		
		job.setJarByClass(BeanRunnerSolrKey.class);
		
		job.setMapperClass(BeanMapper.class);
		job.setReducerClass(BeanReducer.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		String inputPath="/usr/jhc/mapreduce/data";
		String outputPath="/usr/jhc/mapreduce/sorlkey";
		
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
