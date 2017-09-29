package com.example.sort;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Comparator;

import static org.hamcrest.CoreMatchers.*;
import org.junit.Before;
import org.junit.Test;

public class StringSortTest {
	
	String[] strs;

	@Before
	public void setUp() throws Exception {
		strs = new String[] {"5", "3", "10", "3", "6", "7"};
	}

	@Test
	public void testAsc() {
		System.out.println(Arrays.toString(strs));	//before sort
		Arrays.sort(strs);			//default Ascending
		System.out.println(Arrays.toString(strs));	//after sort
		
		assertArrayEquals(new String[] {"10", "3", "3", "5", "6", "7" } , strs);	//요소를 각각 비교
		assertThat(strs, is(new String[] {"10", "3", "3", "5", "6", "7" }));
	}
	
	@Test
	public void testDesc() {
		System.out.println(Arrays.toString(strs));	//before sort
//		Arrays.sort(strs);			//default Ascending
		Arrays.sort(strs, new Comparator<String>() { //sort 비교로직을 통해 바꿀지 놔둘지 정함 (comparator)

			/*
			 * -1(<0), a < b : 그대로
			 *  0, 	 a == b: 그대로
			 *  1(>0), a > b : 교체
			 */
			
			@Override
			public int compare(String o1, String o2) {
				int cmp = o1.compareTo(o2);		//크면 양의 값, 작으면 음의 값, 같으면 0이 나옴
				if(cmp >0)
					cmp = -cmp;		//desc이므로 반대로 리턴
				else if ( cmp ==0)
					cmp = 0;
				else 
					cmp = -cmp;
				return cmp;
//				return -o1.compareTo(o2);
			} 
		});		
		
		System.out.println(Arrays.toString(strs));	//after sort
		
		assertArrayEquals(new String[] {"7", "6", "5", "3", "3", "10"} , strs);	//요소를 각각 비교. 다르면 에러
		assertThat(strs, is(new String[] {"7", "6", "5", "3", "3", "10"}));
	}

}
