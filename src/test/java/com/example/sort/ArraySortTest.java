package com.example.sort;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Comparator;

import static org.hamcrest.CoreMatchers.*;
import org.junit.Before;
import org.junit.Test;

public class ArraySortTest {
	
	Integer[] nums;

	@Before
	public void setUp() throws Exception {
		nums = new Integer[] {5, 3, 10, 3, 6, 7};
	}

	@Test
	public void testAsc() {
		System.out.println(Arrays.toString(nums));	//before sort
		Arrays.sort(nums);			//default Ascending
		System.out.println(Arrays.toString(nums));	//after sort
		
		assertArrayEquals(new Integer[] {3, 3, 5, 6, 7, 10} , nums);	//요소를 각각 비교
	}
	
	@Test
	public void testDesc() {
		System.out.println(Arrays.toString(nums));	//before sort
//		Arrays.sort(nums);			//default Ascending
		Arrays.sort(nums, new Comparator<Integer>() { //sort 비교로직을 통해 바꿀지 놔둘지 정함 (comparator)

			/*
			 * -1 : 그대로
			 * 	0 : 그대로 (같기 때문에)
			 *  1 : 교체
			 */
			
			@Override
			public int compare(Integer o1, Integer o2) {
				//첫 두 요소를 o1, o2에 집어 넣는다. 리턴하는 것이 -1,0이면 바꾸지 않음. 1이면 바꿈. -> 모든 요소 적용 
				if(o1 < o2)
					return 1;
				else if ( o1 == o2)
					return 0;
				else
					return -1;
			} 
		});		
		
		System.out.println(Arrays.toString(nums));	//after sort
		
		assertArrayEquals(new Integer[] {10, 7, 6, 5, 3, 3} , nums);	//요소를 각각 비교. 다르면 에러
		assertThat(nums, is(new Integer[] {10, 7, 6, 5, 3, 3}));		//위와 같은 의미
	}

}
