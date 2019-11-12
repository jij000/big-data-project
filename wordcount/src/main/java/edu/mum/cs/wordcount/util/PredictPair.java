package edu.mum.cs.wordcount.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.google.common.primitives.Bytes;

public class PredictPair extends BinaryComparable implements WritableComparable<BinaryComparable> {
	String customerId;
	String item1;
	String item2;

	public PredictPair() {
	}

	public PredictPair(String customerId, String x1, String x2) {
		this.customerId = customerId;
		this.item1 = x1;
		this.item2 = x2;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
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
		out.writeUTF(this.customerId);
		out.writeUTF(this.item1);
		out.writeUTF(this.item2);
	}

	public void readFields(DataInput in) throws IOException {
		this.customerId = in.readUTF();
		this.item1 = in.readUTF();
		this.item2 = in.readUTF();

	}

	@Override
	public int getLength() {
		return this.customerId.length() + this.item1.length() + this.item2.length();
	}

	@Override
	public String toString() {
		return "[customerId=" + customerId + ", item=(" + item1 + ", " + item2 + ")]";
	}

	@Override
	public byte[] getBytes() {
		return Bytes.concat(this.customerId.getBytes(), this.item1.getBytes(), this.item2.getBytes());
	}

	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(PredictPair.class);
		}

		public int compare(PredictPair a, PredictPair b) {
			if (a.getCustomerId().compareTo(b.getCustomerId()) == 0) {
				if (a.getItem1().compareTo(b.getItem1()) == 0) {
					return a.getItem2().compareTo(b.getItem2());
				} else {
					return a.getItem1().compareTo(b.getItem1());
				}
			} else {
				return a.getCustomerId().compareTo(b.getCustomerId());
			}
		}
	}
}
