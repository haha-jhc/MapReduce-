package com.jhc.flowsum;

import java.io.IOException;

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

import com.jhc.flowsum.FlowBean;
import com.jhc.flowsum.FlowCount;



public class FlowCountSort {
	
	static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
		
		FlowBean bean=new FlowBean();
		Text v=new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
				throws IOException, InterruptedException {
			//�õ�������һ��ͳ�Ƴ������������Ѿ��Ǹ��ֻ��ŵ���������Ϣ
			String line=value.toString();
			
			String[] fields=line.split("\t");
			String phoneNbr=fields[0];
			long upFlow=Long.parseLong(fields[1]);
			long dFlow=Long.parseLong(fields[2]);
			
			bean.set(upFlow, dFlow);
			v.set(phoneNbr);
			
			context.write(bean, v);//д��������Ҫ���л�
			
		}
	} 
	
	static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
		
		@Override
		protected void reduce(FlowBean bean, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			
			context.write(values.iterator().next(), bean);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resoucemanager", "mini1");
		Job job=Job.getInstance(conf);
		
		//ָ���������jar������·��
		job.setJarByClass(FlowCount.class);
		
		//ָ����ҵ��jobʹ�õ�mapperҵ����
		job.setMapperClass(FlowCountSortMapper.class);
		job.setReducerClass(FlowCountSortReducer.class);
		
		//ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
//		//ָ�������Զ�������ݷ�����
//		job.setPartitionerClass(ProvincePartitioner.class);
//		//ͬʱָ����Ӧ�ڷ���������reducetask
//		job.setNumReduceTasks(5);
		
		//ָ��������������ݵ�kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//ָ��job������ԭʼ�ļ�����Ŀ¼
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//ָ��job������������Ŀ¼
		Path outPath=new Path(args[1]);
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath,true);
		}
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//��job�����õ���ز������Լ�job���õ�Java������jar�����ύ��yarn����
		//job.submit();
		boolean res =job.waitForCompletion(true);
		System.exit(res?0:1);
	}

}
