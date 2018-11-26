package com.jhc.mr.secondsolr;

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



public class BeanRunnerSolr {

	static class BeanMapper extends Mapper<LongWritable, Text, Bean,Text>{
		Bean bean = new Bean();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Bean, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line,"\t");
			
			String timestamp = fields[0];
			String ip=fields[1];
			String domain=fields[4];
			String Width=fields[5];
			Text tx = new Text(timestamp+"\t"+domain+"\t"+ip);
			bean.set(Long.parseLong(Width), Long.parseLong(timestamp));
			context.write(bean, tx);
		}
	}
	
	static class BeanReduce extends Reducer<Bean, Text, LongWritable, Text>{
		@Override
		protected void reduce(Bean key, Iterable<Text> values, Reducer<Bean, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			for(Text value:values){
				context.write(new LongWritable(key.getWidth()), value);
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf =new Configuration();
		Job job =Job.getInstance(conf);
		
		job.setJarByClass(BeanRunnerSolr.class);
		
		job.setMapperClass(BeanMapper.class);
		job.setReducerClass(BeanReduce.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		String inputPath="/usr/jhc/mapreduce/data";
		String outputPath="/usr/jhc/mapreduce/secondsolr";
		
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
