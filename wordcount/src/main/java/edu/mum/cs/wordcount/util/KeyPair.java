package edu.mum.cs.wordcount.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.google.common.primitives.Bytes;

public class KeyPair extends BinaryComparable implements WritableComparable<BinaryComparable> {
	String item1;
	String item2;

	public KeyPair() {
	}

	public KeyPair(String x1, String x2) {
		this.item1 = x1;
		this.item2 = x2;
	}

	public String getItem1() {
		return item1;
	}

	public void setItem1(String item1) {
		this.item1 = item1;
	}

	public String getItem2() {
		return item2;
	}

	public void setItem2(String item2) {
		this.item2 = item2;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.item1);
		out.writeUTF(this.item2);
	}

	public void readFields(DataInput in) throws IOException {
		this.item1 = in.readUTF();
		this.item2 = in.readUTF();

	}

	@Override
	public int getLength() {
		return this.item1.length() + this.item2.length();
	}

	@Override
	public String toString() {
		return "item=(" + item1 + ", " + item2 + ")";
	}

	@Override
	public byte[] getBytes() {
		return Bytes.concat(this.item1.getBytes(), this.item2.getBytes());
	}

	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(KeyPair.class);
		}

		public int compare(KeyPair a, KeyPair b) {
			if (a.getItem1().compareTo(b.getItem1()) == 0) {
				return a.getItem2().compareTo(b.getItem2());
			} else {
				return a.getItem1().compareTo(b.getItem1());
			}
		}
	}
}
