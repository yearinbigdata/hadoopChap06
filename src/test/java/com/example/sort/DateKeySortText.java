package com.example.sort;

import static org.junit.Assert.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import static org.hamcrest.CoreMatchers.*;

import org.junit.Before;
import org.junit.Test;

public class DateKeySortText {

	static class DateKey implements Comparable<DateKey> {

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
			return Objects.hash(year, month); // hashCode return. year, month를
												// 기준 삼아 hashcode가 만들어진다.
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

	}

	List<DateKey> list;

	@Before
	public void setUp() throws Exception {
		list = new ArrayList<>();

		Random r = new Random();

		int total = 10;
		for (int i = 0; i < total; i++) {
			String year = r.nextInt(22) + 1987 + "";
			Integer month = r.nextInt(12) + 1;
			list.add(new DateKey(year, month));
		}
	}

	@Test
	public void test() {
		System.out.println(list);
//		list.sort(null);
		Collections.sort(list);
		System.out.println(list);
	}

	@Test
	public void test2() {
		LocalDateTime start = LocalDateTime.now();
		list.sort(null);
		LocalDateTime end = LocalDateTime.now();

		System.out.println(Duration.between(start, end));
	}

	@Test
	public void test3() {
		Map<DateKey, Integer> map = new HashMap<>();
		// map에 들어가 있음
		// map사용: HashCode, Equals가 정의되어 있어야 한다.
		map.put(new DateKey("1987", 10), 100);
		map.put(new DateKey("1988", 11), 200);
		map.put(new DateKey("1989", 12), 300);

		System.out.println("departure cnt = " + map.get(new DateKey("1987", 10)));
		System.out.println("departure cnt = " + map.get(new DateKey("1988", 11)));
		System.out.println("departure cnt = " + map.get(new DateKey("1989", 12)));

		assertThat(map.get(new DateKey("1987", 10)), is(100));
		assertThat(map.get(new DateKey("1988", 11)), is(200));
		assertThat(map.get(new DateKey("1989", 12)), is(300));

	}

}
