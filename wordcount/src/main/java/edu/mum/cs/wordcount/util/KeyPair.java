package edu.mum.cs.wordcount.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.google.common.primitives.Bytes;

public class KeyPair extends BinaryComparable implements WritableComparable<BinaryComparable> {
	String x1;
	String x2;

	public KeyPair() {
	}

	public KeyPair(String x1, String x2) {
		this.x1 = x1;
		this.x2 = x2;
	}

	public String getX1() {
		return x1;
	}

	public void setX1(String x1) {
		this.x1 = x1;
	}

	public String getX2() {
		return x2;
	}

	public void setX2(String x2) {
		this.x2 = x2;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(x1);
		out.writeUTF(x2);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.x1 = in.readUTF();
		this.x2 = in.readUTF();

	}

	@Override
	public int getLength() {
		// TODO Auto-generated method stub
		return this.x1.length() + this.x2.length();
	}

	@Override
	public String toString() {
		return "KeyPair [x1=" + x1 + ", x2=" + x2 + "]";
	}

	@Override
	public byte[] getBytes() {
		// TODO Auto-generated method stub
		return Bytes.concat(this.x1.getBytes(), this.x2.getBytes());
	}

	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(KeyPair.class);
		}

		public int compare(KeyPair a, KeyPair b) {
			if (a.getX1().compareTo(b.getX1()) == 0) {
				return a.getX2().compareTo(b.getX2());
			} else {
				return a.getX1().compareTo(b.getX1());
			}
		}

	}
}
