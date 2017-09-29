package com.example.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateKeyComparator extends WritableComparator  {
//객체가 comparable한데, 다른 기준으로 sort하고자 할 때, comparator를 사용자지정으로 만든다
	public DateKeyComparator() {
		super(DateKey.class, true);
	}//디폴트 생성자가 없기 때문에 사용하기 위한 datekey를 주고, true를 통해 생성하라 한 것.
	
//	@SuppressWarnings("rawtypes")
//	@Override
//		public int compare( WritableComparable a, WritableComparable b) {
//			
//		return -a.compareTo(b);
//		
//		
//		
//		}
	@Override  //year는 desc, month는 asc
		public int compare(WritableComparable a, WritableComparable b) {
			DateKey k1 = (DateKey)a;		//부모클래스로 a, b가 들어오므로 자식클래스로 다시 typeCasting해야한다.
			DateKey k2 = (DateKey)b;
			
			//책 코드
//			int cmp = k1.getYear().compareTo(k2.getYear());
//			if (cmp != 0) {
//				return -cmp;
//			}else
//				return k1.getMonth().compareTo(k2.getMonth());
			
			//선생님 코드
			int cmp=0;
			if(k1.getYear().equals(k2.getYear())){
				cmp = k1.getMonth().compareTo(k2.getMonth());
			}else {
				cmp = -k1.getMonth().compareTo(k2.getMonth());
			}
			return cmp;
		}
	
	
}
