package com.example.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateKeyGroupComparator extends WritableComparator{
	
	public DateKeyGroupComparator(){
		super(DateKey.class, true);
	}
	
	@Override	//하나의 연도를 그룹으로 만들어서 비교기
	public int compare(WritableComparable a, WritableComparable b) {
		DateKey k1 = (DateKey)a;
		DateKey k2 = (DateKey) b;
		
		int cmp = k1.getYear().compareTo(k2.getYear());
		return cmp;
		
//		return k1.getYear().equals(k2.getYear()) ? 0 : 1 ;	//같으면 0, 아니면 1
		
	}

}
