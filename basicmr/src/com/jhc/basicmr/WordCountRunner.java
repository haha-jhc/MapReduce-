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
				//�ۼ�ÿһ��value
				counter +=value.get();
			}
			context.write(key, new LongWritable(counter));			
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		FileSystem fs= FileSystem.get(conf);
		Job job=Job.getInstance(conf);
		//���ñ���job��ҵ���ڵ�jar��
		job.setJarByClass(WordCountRunner.class);
		//ָ��������ҵʹ�õ�Mapper�����ĸ�
		job.setMapperClass(WordCountMapper.class);
		//ָ��������ҵʹ�õ�Reduce�����ĸ�
		job.setReducerClass(WordCountReducer.class);
		//ָ��Mapper����������key������
		job.setMapOutputKeyClass(Text.class);
		//ָ��Mapper����������value������
		job.setMapOutputValueClass(LongWritable.class);
		//ָ��Reduce����������key������
		job.setOutputKeyClass(Text.class);
		//ָ��Reduce����������value������
		job.setOutputValueClass(LongWritable.class);
		
		job.setNumReduceTasks(1); //����reduce�ĸ���
		
		String inputPath = "/usr/jhc/mapreduce/data";
		String outputPath = "/usr/jhc/mapreduce/wordcount";
		
		//�ж�output �ļ����Ƿ����
		Path path=new Path(outputPath);
		FileSystem fileSystem =path.getFileSystem(conf);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path,true);
		}
		
		// //����job��ҵҪ�����ԭʼ��������·��
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		// //����job��ҵ������������·��
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		//�ύ������ҵ
		job.waitForCompletion(true);
		
	}

}
