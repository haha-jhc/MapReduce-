package com.jhc.wcdemo;



import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/*
 * KEYIN,VALUEIN 对应mapper输出的KEYOUT,VALUEOUT类型对应
 * 
 * KEYOUT,VALUEOUT是自定义reduce逻辑处理结果的输出类型
 * KEYOUT是单词
 * VALUEOUT是总次数
 * 
 */
public class WordcountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

	/*
	 * 输入参数key,是一组相同单词kv对的key,如<hello,1><world,1>
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
