package com.example.sort;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Comparator;

import static org.hamcrest.CoreMatchers.*;
import org.junit.Before;
import org.junit.Test;

public class BeanSortTest {
	
	
	static class DateKey implements Comparable<DateKey>{	//bean
		
		private String year;
		private String month;
		
		@Override
			public String toString() {
				return year + "," + month;
			}
		
		@Override
			public boolean equals(Object obj) {
				DateKey dk = (DateKey)obj;
			
				return year.equals(dk.year) && month.equals(dk.month);
			}
		
		@Override
			public int compareTo(DateKey o) {
				int cmp = this.year.compareTo(o.year);
				
				if (cmp == 0){	//연도가 같을 때에만 month 비교하면 된다 -> 월별 sort
					int m1 = Integer.parseInt(this.month);
					int m2 = Integer.parseInt(o.month);
					
					if (m1 < m2)
						cmp = -1;
					else if (m1 == m2)
						cmp = 0;
					else
						cmp = 1;
				}
			
				return cmp;
			}
		
		public DateKey() {
			
		}
		
		public DateKey(String year, String month) {
			this.year = year;
			this.month = month;
		}
		
		public String getYear() {
			return year;
		}
		public void setYear(String year) {
			this.year = year;
		}
		public String getMonth() {
			return month;
		}
		public void setMonth(String month) {
			this.month = month;
		}
		
	}
	
	DateKey[] keys; 

	@Before
	public void setUp() throws Exception {
		keys = new DateKey[] {
				new DateKey("2008", "10"),
				new DateKey("2008", "5"),
				new DateKey("2007", "10"),
				new DateKey("2017", "3"),
				new DateKey("2017", "7"),
				new DateKey("2007", "7"),
		};
	}

	@Test
	public void testAsc() {
		System.out.println(Arrays.toString(keys));	//toString 재정의
		Arrays.sort(keys);
		System.out.println(Arrays.toString(keys));
		
		assertArrayEquals(new DateKey[] {		//equals 재정의
				new DateKey("2007", "7"),				
				new DateKey("2007", "10"),
				new DateKey("2008", "5"),
				new DateKey("2008", "10"),
				new DateKey("2017", "3"),
				new DateKey("2017", "7")
		}, keys);
		assertThat(keys, is(new DateKey[] {		//equals 재정의
				new DateKey("2007", "7"),				
				new DateKey("2007", "10"),
				new DateKey("2008", "5"),
				new DateKey("2008", "10"),
				new DateKey("2017", "3"),
				new DateKey("2017", "7")
		}));
	}
	
	@Test
	public void testDesc() {
		System.out.println(Arrays.toString(keys));	//toString 재정의
//		Arrays.sort(keys);
		Arrays.sort(keys, new Comparator<DateKey>() {

			@Override
			public int compare(DateKey o1, DateKey o2) {
				return -o1.compareTo(o2);	//-로 뒤집어서 descending으로 바뀜
			}
		});
		System.out.println(Arrays.toString(keys));
		
		assertArrayEquals(new DateKey[] {		
				new DateKey("2017", "7"),
				new DateKey("2017", "3"),
				new DateKey("2008", "10"),
				new DateKey("2008", "5"),
				new DateKey("2007", "10"),
				new DateKey("2007", "7"),				
		}, keys);
		assertThat(keys, is(new DateKey[] {		
				new DateKey("2017", "7"),
				new DateKey("2017", "3"),
				new DateKey("2008", "10"),
				new DateKey("2008", "5"),
				new DateKey("2007", "10"),
				new DateKey("2007", "7"),				
		}));
	}
	
	@Test
	public void testDesc2() {
		System.out.println(Arrays.toString(keys));	//toString 재정의
//		Arrays.sort(keys);
		Arrays.sort(keys, new Comparator<DateKey>() {

			@Override
			public int compare(DateKey o1, DateKey o2) {
	int cmp = -o1.year.compareTo(o2.year);
				
				if (cmp == 0){	//연도가 같을 때에만 month 비교하면 된다 -> 월별 sort
					int m1 = Integer.parseInt(o1.month);
					int m2 = Integer.parseInt(o2.month);
					
					if (m1 < m2)
						cmp = -1;
					else if (m1 == m2)
						cmp = 0;
					else
						cmp = 1;
				}
			
				return cmp;

			}
		});
		System.out.println(Arrays.toString(keys));
		
		assertArrayEquals(new DateKey[] {		//equals 재정의
				new DateKey("2017", "3"),
				new DateKey("2017", "7"),
				new DateKey("2008", "5"),
				new DateKey("2008", "10"),
				new DateKey("2007", "7"),				
				new DateKey("2007", "10"),
		}, keys);
		assertThat(keys, is(new DateKey[] {		//equals 재정의
				new DateKey("2017", "3"),
				new DateKey("2017", "7"),
				new DateKey("2008", "5"),
				new DateKey("2008", "10"),
				new DateKey("2007", "7"),				
				new DateKey("2007", "10"),
		}));
	}

}
