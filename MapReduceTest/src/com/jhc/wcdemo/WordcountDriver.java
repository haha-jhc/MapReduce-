package com.jhc.wcdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/*
 *�൱��һ��yarn��Ⱥ�Ŀͻ���
 *��Ҫ�ڴ˷�װ���ǵ�mr�����������в�����ָ��jar��
 *����ύ��yarn
 * 
 * 
 */
public class WordcountDriver {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf=new Configuration();
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resoucemanager", "mini1");
		Job job=Job.getInstance(conf);
		
		//ָ���������jar������·��
		job.setJarByClass(WordcountDriver.class);
		
		//ָ����ҵ��jobʹ�õ�mapperҵ����
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		
		//ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//ָ��������������ݵ�kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//ָ��job������ԭʼ�ļ�����Ŀ¼
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//ָ��job������������Ŀ¼
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//��job�����õ���ز������Լ�job���õ�Java������jar�����ύ��yarn����
		//job.submit();
		boolean res =job.waitForCompletion(true);
		System.exit(res?0:1);
	}

}
