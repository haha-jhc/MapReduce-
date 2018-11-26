package com.jhc.mr.partition01;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Bean implements Writable {

	private String timestamp;
	private String ip;
	
	//�����л�ʱ��Ҫ���䣬������һ���޲εĹ��캯��
	public Bean() {
		// TODO Auto-generated constructor stub
	}
	//����һ���вι��캯�����Ḳ��Ĭ�ϵ��޲ι��캯��
	public Bean(String timestamp,String ip){
		this.set(timestamp, ip);
	}
	
	public void set(String timestamp,String ip) {
		this.timestamp=timestamp;
		this.ip=ip;
	}
	
	//�����л�����
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		timestamp=in.readUTF();
		ip=in.readUTF();
	}

	//���л�����
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
