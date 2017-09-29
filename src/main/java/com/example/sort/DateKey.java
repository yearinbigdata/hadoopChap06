package com.example.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

//public class DateKey implements Comparable<DateKey>, Writable {
public class DateKey implements WritableComparable<DateKey>{

	String year;
	Integer month;

	public DateKey() {
	}

	public DateKey(String year, Integer month) {
		this.year = year;
		this.month = month;
	}

	@Override
	public String toString() {
		return year + "," + month;
	}

	@Override
	public boolean equals(Object obj) {
		DateKey dateKey = (DateKey) obj;
		return this.year.equals(dateKey.year) && this.month.equals(dateKey.month);
	}

	@Override
	public int hashCode() {
		return Objects.hash(year, month); // hashCode return. year, month를 기준 삼아
											// hashcode가 만들어진다.
	}

	@Override
	public int compareTo(DateKey o) {
		int cmp = year.compareTo(o.year);

		if (cmp == 0) {
			cmp = month.compareTo(o.month);
		}
		return cmp;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, year);
//		out.writeUTF(year);
		out.writeInt(month);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year = WritableUtils.readString(in);
//		year = in.readUTF();
		month = in.readInt();
		
	}

}
