package com.jhc.mr.partitionsolr;

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

public class BeanRunnerPartitionsolr {

	static class BeanMapper extends Mapper<LongWritable, Text, Bean, Text>{
		public Bean bean =new Bean();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Bean, Text>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] fields= StringUtils.split(line,"\t");
			String timestamp = fields[0];
			String ip =fields[1];
			String domain = fields[4];
			String Width = fields[5];
			Text tx = new Text(timestamp+"\t"+ip);
			bean.set(domain, Long.parseLong(Width));
			context.write(bean, tx);
		}
	}
	
	static class BeanReduce extends Reducer<Bean, Text, Bean, Text>{
		@Override
		protected void reduce(Bean key, Iterable<Text> values, Reducer<Bean, Text, Bean, Text>.Context context)
				throws IOException, InterruptedException {
			for(Text value:values){
				context.write(key, value);
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(BeanRunnerPartitionsolr.class);
		
		job.setMapperClass(BeanMapper.class);
		job.setReducerClass(BeanReduce.class);
		
		job.setPartitionerClass(ProvincePartitioner.class);
		job.setNumReduceTasks(6);
		
		job.setOutputKeyClass(Bean.class);
		job.setOutputValueClass(Text.class);
		
		String inputPath ="/usr/jhc/mapreduce/data";
		String outputPath = "/usr/jhc/mapreduce/solrandpartition";
		
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
