package com.jhc.mr.partition01;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, Bean>{
	private static HashMap<String, Integer> provinceMap = new HashMap<String,Integer>();

	static{
		provinceMap.put("si2.mfniu.com", 0);
		provinceMap.put("si.mfniu.com", 1);
		provinceMap.put("www.88mf.com", 2);
		provinceMap.put("pc.mfniu.com", 3);
		provinceMap.put("pc.88mf.com", 4);
	}
	
	@Override
	public int getPartition(Text key, Bean value, int numPartitions) {
		
		Integer province =null;
		province =provinceMap.get(key.toString());
		if (province==null) {
			province = 5;
		}
		return province;
	}
	
}
