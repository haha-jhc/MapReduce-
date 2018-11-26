package com.jhc.mr.secondsolr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;



public class Bean implements Writable {

	private long Width;
	private long timestamp;
	
	public Bean(){
		
	}
	
	public Bean(long Width,long timestamp) {
		this.set(Width, timestamp);
	}
	
	public void set(long Width,long timestamp) {
		this.Width=Width;
		this.timestamp=timestamp;
	}
	
	public long getWidth() {
		return this.Width;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		Width=in.readLong();
		timestamp=in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(Width);
		out.writeLong(timestamp);
	}
	
	public int compareTo(Bean key){
		long min = Width - key.Width;
		if(min !=0){
			return (int) min;
		}else {
			return (int) (timestamp - key.timestamp);
		}	
	}

}
