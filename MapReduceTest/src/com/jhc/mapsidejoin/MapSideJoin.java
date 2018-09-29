package com.jhc.mapsidejoin;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MapSideJoin {
	
	static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		//��һ��HashMap�����ر����Ʒ��Ϣ��
		Map<String,String>pdInfoMap=new HashMap<String,String>();
		Text k =new Text();
		//setup��maptask��������֮ǰ����һ�Σ�����������һЩ��ʼ������
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream("pdts.txt")));
			String line;
			while (StringUtils.isNotEmpty(line=br.readLine())) {
				String[] fields=line.split(",");
				pdInfoMap.put(fields[0], fields[1]);
			}
			br.close();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String orderLine=value.toString();
			String[] fields=orderLine.split("\t");
			String pdName =pdInfoMap.get(fields[1]);
			k.set(orderLine+"\t"+pdName);
			context.write(k, NullWritable.get());
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf=new Configuration();
		
		Job job= Job.getInstance(conf);
		
		job.setJarByClass(MapSideJoin.class);
		
		job.setMapperClass(MapSideJoinMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job , new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		/*job.addArchiveToClassPath(archive);*/  //����jar����task���нڵ��classpath
		/*job.addFileToClassPath(file);*/	//������ͨ�ļ���task���нڵ��classpath
		/*job.addCacheArchive(url);*/	//����ѹ�����ļ���task���нڵ�Ĺ���Ŀ¼
		//����Ʒ���ļ����浽task�����ڵ�Ĺ���Ŀ¼��ȥ
		job.addCacheFile(new URI("hdfs://wordcount/task/pdt.txt"));	//������ͨ�ļ���task���нڵ�Ĺ���Ŀ¼
		
		//map��join���߼�����Ҫreduce�׶�
		
		
		boolean res=job.waitForCompletion(true);
		System.exit(res?0:1);
	}

}
