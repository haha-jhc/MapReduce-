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
			//拿到的是上一个统计程序输出结果，已经是各手机号的总流量信息
			String line=value.toString();
			
			String[] fields=line.split("\t");
			String phoneNbr=fields[0];
			long upFlow=Long.parseLong(fields[1]);
			long dFlow=Long.parseLong(fields[2]);
			
			bean.set(upFlow, dFlow);
			v.set(phoneNbr);
			
			context.write(bean, v);//写出的数据要序列化
			
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
		
		//指定本程序的jar包所在路径
		job.setJarByClass(FlowCount.class);
		
		//指定本业务job使用的mapper业务类
		job.setMapperClass(FlowCountSortMapper.class);
		job.setReducerClass(FlowCountSortReducer.class);
		
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
//		//指定我们自定义的数据分区器
//		job.setPartitionerClass(ProvincePartitioner.class);
//		//同时指定相应于分区数量的reducetask
//		job.setNumReduceTasks(5);
		
		//指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//指定job的输出结果所在目录
		Path outPath=new Path(args[1]);
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath,true);
		}
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//将job中配置的相关参数，以及job所用的Java类所在jar包，提交给yarn运行
		//job.submit();
		boolean res =job.waitForCompletion(true);
		System.exit(res?0:1);
	}

}
