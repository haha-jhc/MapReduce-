package com.jhc.mr.partition01;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Bean implements Writable {

	private String timestamp;
	private String ip;
	
	//反序列化时需要反射，必须有一个无参的构造函数
	public Bean() {
		// TODO Auto-generated constructor stub
	}
	//定义一个有参构造函数，会覆盖默认的无参构造函数
	public Bean(String timestamp,String ip){
		this.set(timestamp, ip);
	}
	
	public void set(String timestamp,String ip) {
		this.timestamp=timestamp;
		this.ip=ip;
	}
	
	//反序列化方法
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		timestamp=in.readUTF();
		ip=in.readUTF();
	}

	//序列化方法
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(timestamp);
		out.writeUTF(ip);
	}
	
	public String toString() {
		return timestamp+"\t"+ip;
	}

}
