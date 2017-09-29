package com.example.delay.mos;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.example.parser.DelayCounters;
import com.example.sort.DateKey;

public class DelayCountMapperTest {

	MapDriver<LongWritable, Text, DateKey, IntWritable> map;
	
                 //  year, month, dayofmonth,.......,uniqueCarrier,.........,arrDelay, depDelay, distance
	String recode1 = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,NA,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String recode2 = "2008,9,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,40,NA,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";
	String recode3 = "2008,9,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,40,34,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";
	String recode4 = "2008,9,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,NA,NA,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";

	@Before
	public void setUp() throws Exception {
		map = MapDriver.newMapDriver(new DelayCountMapper());
	}

	@After
	public void tearDown() throws Exception {
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testMap1() throws IOException {
		
		map.withInput(new LongWritable(0), new Text(recode1));
//		map.withOutput(new Text("D,2008,1"), new IntWritable(1));
		map.withOutput(new DateKey("D,2008", 1), new IntWritable(1));
		map.withCounter(DelayCounters.delay_departure, 1);
		
		map.runTest();
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testMap2() throws IOException {
		
		map.withInput(new LongWritable(0), new Text(recode2));
//		map.withOutput(new Text("A,2008,9"), new IntWritable(1));
		map.withOutput(new DateKey("A,2008",9), new IntWritable(1));
		map.withCounter(DelayCounters.delay_arrival, 1);
		
		map.runTest();
	}
	

	@SuppressWarnings("deprecation")
	@Test
	public void testMap3() throws IOException {
		
		map.withInput(new LongWritable(0), new Text(recode3));
//		map.withOutput(new Text("D,2008,9"), new IntWritable(1));
//		map.withOutput(new Text("A,2008,9"), new IntWritable(1));
		map.withOutput(new DateKey("D,2008",9), new IntWritable(1));
		map.withOutput(new DateKey("A,2008",9), new IntWritable(1));
		map.withCounter(DelayCounters.delay_departure, 1);
		map.withCounter(DelayCounters.delay_arrival, 1);
		
		map.runTest();
		
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testMap4() throws IOException {
		
		map.withInput(new LongWritable(0), new Text(recode4));
//		map.withOutput(new Text("2008,9"), new IntWritable(1));
		map.withCounter(DelayCounters.delay_departure, 0);
		
		map.runTest();
	}

}
