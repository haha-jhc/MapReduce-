package com.jhc.wcdemo;



import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/*
 * KEYIN,VALUEIN ��Ӧmapper�����KEYOUT,VALUEOUT���Ͷ�Ӧ
 * 
 * KEYOUT,VALUEOUT���Զ���reduce�߼����������������
 * KEYOUT�ǵ���
 * VALUEOUT���ܴ���
 * 
 */
public class WordcountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

	/*
	 * �������key,��һ����ͬ����kv�Ե�key,��<hello,1><world,1>
	 * 
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) 
			throws IOException, InterruptedException {
		
		int count=0;
		
		for(IntWritable value:values){
			count+=value.get();
		}
		
		context.write(key, new IntWritable(count));
	}
}
