package com.jhc.provinceflow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class FlowCount {
	
	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			//�з��ֶ�
			String[] fields=line.split("\t");
			//ȡ���ֻ���
			String phoneNbr=fields[1];
			//ȡ��������������������
			long upFlow=Long.parseLong(fields[fields.length-3]);
			long dFlow=Long.parseLong(fields[fields.length-2]);
			context.write(new Text(phoneNbr), new FlowBean(upFlow, dFlow));
		}
	}
	static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			long sum_upFLow=0;
			long sum_dFlow=0;
			
			//��������bean�������е��������������������ֱ��ۼ�
			for (FlowBean bean:values) {
				sum_upFLow+=bean.getUpFlow();
				sum_dFlow+=bean.getdFlow();
			}
			FlowBean resultBean=new FlowBean(sum_upFLow, sum_dFlow);
			context.write(key, resultBean);
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
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		//ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//ָ�������Զ�������ݷ�����
		job.setPartitionerClass(ProvincePartitioner.class);
		//ͬʱָ����Ӧ�ڷ���������reducetask
		job.setNumReduceTasks(5);
		
		//ָ��������������ݵ�kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
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
