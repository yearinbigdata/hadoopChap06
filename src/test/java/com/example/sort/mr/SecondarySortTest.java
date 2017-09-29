package com.example.sort.mr;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.example.sort.DateKey;
import com.example.sort.DateKeyComparator;

public class SecondarySortTest  {
	
	static class MyMapper extends Mapper<Text, IntWritable, DateKey, IntWritable>{
		//MapReducer에서 사용할 수 있는 타입들
		/*
		 * key의 요구사항 (인터페이스 구현)
		 * 1.  writable(serialization 쓸 수 있어야 함)
		 * 2.  comparable(비교 가능해야 하므로)
		 * 
		 * value의 요구사항
		 * 1. writable
		 */
		
		@Override
		protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

			String[] strs = key.toString().split(",");
			String year = strs[0];
			Integer month = Integer.parseInt(strs[1]);
			
			context.write(new DateKey(year, month), value);
			
		}
		
	}
	
	static class MyReducer extends Reducer<DateKey, IntWritable, DateKey, IntWritable>{
		
		@Override
		protected void reduce(DateKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
			int sum = 0;
			for (IntWritable v: values){
				sum += v.get();
			}
			
			context.write(key, new IntWritable(sum));
		
		}
		
	}
										//맵의 출력키이자 리듀스의 입력키 2개는 중복. 따라서 6가지 나온다
	MapReduceDriver<Text, IntWritable, DateKey, IntWritable, DateKey, IntWritable> mapDriver;
							
	@Before
	public void setUp() throws Exception {
		MyMapper mapper = new MyMapper();
		MyReducer reducer = new MyReducer();
		
		mapDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		//간단하게 내부 클래스로 정의
	}

	@Test
	public void testMapReduceAsc() throws IOException {
		
		mapDriver.withInput(new Text("2008,10"), new IntWritable(100));
		mapDriver.withInput(new Text("1987,8"), new IntWritable(100));
		mapDriver.withInput(new Text("2008,10"), new IntWritable(100));
		mapDriver.withInput(new Text("1987,8"), new IntWritable(100));
		mapDriver.withInput(new Text("2008,9"), new IntWritable(100));
		mapDriver.withOutput(new DateKey("1987", 8), new IntWritable(200));
		mapDriver.withOutput(new DateKey("2008", 9), new IntWritable(100));
		mapDriver.withOutput(new DateKey("2008", 10), new IntWritable(200));

		mapDriver.runTest();
		
	}
	
	@Test
	public void testMapReduceDesc() throws IOException {
				
//		mapDriver.setKeyComparator(new DateKeyComparator());
		mapDriver.setKeyOrderComparator(new DateKeyComparator());
		
		mapDriver.withInput(new Text("2008,10"), new IntWritable(100));
		mapDriver.withInput(new Text("1987,8"), new IntWritable(100));
		mapDriver.withInput(new Text("2008,10"), new IntWritable(100));
		mapDriver.withInput(new Text("1987,8"), new IntWritable(100));
		mapDriver.withInput(new Text("2008,9"), new IntWritable(100));
		mapDriver.withOutput(new DateKey("2008", 9), new IntWritable(100));
		mapDriver.withOutput(new DateKey("2008", 10), new IntWritable(200));
		mapDriver.withOutput(new DateKey("1987", 8), new IntWritable(200));

		mapDriver.runTest();
		
	}

}
