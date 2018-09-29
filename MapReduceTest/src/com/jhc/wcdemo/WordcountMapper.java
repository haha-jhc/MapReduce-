package com.jhc.wcdemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * KEYIN:Ĭ������£���mr�����������һ���ı���ʼƫ����,Long,
 * ��Hadoop�����Լ��ĸ���������л��ӿ�,ֱ����LongWritable
 * 
 * VALUEIN:Ĭ������£���mr�����������һ���ı�������,String,ͬ����Text
 * 
 * KEYOUT,���û��Զ����߼��������֮����������е�key,�ڴ˴��ǵ��ʣ�String,ͬ����Text
 * VALUEOUT:���û��Զ����߼��������֮����������е�value,�ڴ˴��ǵ��ʴ�����Integer,ͬ����IntWritable
 * 
 * 
 * 
 */
public class WordcountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

	/*
	 * map�׶ε�ҵ���߼���д���Զ����map()������ 
	 * maptask���ÿһ���������ݵ���һ�������Զ����map()����
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//��maptask�������ǵ��ı�����ת��String
		String line= value.toString();
		//���ݿո���һ���зֳɵ���
		String[] words= line.split(" ");
		
		//���������Ϊ<���ʣ�1>
		for(String word:words){
			/*
			 *��������Ϊkey,������1��Ϊvalue���Ա��ں��������ݷַ���
			 *���Ը��ݵ��ʷַ����Ա���ͬ���ʻᵽ��ͬ��reduce task
			 */
			context.write(new Text(word), new IntWritable(1));
		}
		
	}
	
	
}
