package edu.mum.cs.wordcount.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class MyPair implements WritableComparable<LongWritable> {
	Long val;
	Long cnt;
	
	public MyPair() {}
	
	public MyPair(Long val, Long cnt) {
		this.val = val;
		this.cnt = cnt;
	}

	public Long getVal() {
		return val;
	}

	public void setVal(Long val) {
		this.val = val;
	}

	public Long getCnt() {
		return cnt;
	}

	public void setCnt(Long cnt) {
		this.cnt = cnt;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(val);
		out.writeLong(cnt);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		val = in.readLong();
		cnt = in.readLong();
	}

	public int compareTo(LongWritable arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

}
