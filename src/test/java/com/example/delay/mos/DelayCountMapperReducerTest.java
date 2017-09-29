package com.example.delay.mos;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.example.parser.DelayCounters;
import com.example.sort.DateKey;
import com.example.sort.DateKeyComparator;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DelayCountReducer.class, MultipleOutputs.class})
public class DelayCountMapperReducerTest {
	
	MapReduceDriver<LongWritable, Text, DateKey, IntWritable, Text, IntWritable> mapReduce;
	
    //               year, month, dayofmonth,.......,uniqueCarrier,.........,arrDelay, depDelay, distance
	String recode1 = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,34,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String recode2 = "2008,10,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,60,NA,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";
	String recode3 = "1987,10,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,34,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String recode4 = "1987,7,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,NA,50,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";

	@Before
	public void setUp() throws Exception {
		mapReduce = MapReduceDriver.newMapReduceDriver(new DelayCountMapper(), new DelayCountReducer());
	}

	@After
	public void tearDown() throws Exception {
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testMapReduce1() throws IOException {
		mapReduce.withInput(new LongWritable(), new Text(recode1));
		mapReduce.withInput(new LongWritable(), new Text(recode2));
		mapReduce.withInput(new LongWritable(), new Text(recode3));
		mapReduce.withInput(new LongWritable(), new Text(recode4));
		mapReduce.withOutput(new Text("A,1987,10"), new IntWritable(1));
		mapReduce.withOutput(new Text("A,2008,1"), new IntWritable(1));
		mapReduce.withOutput(new Text("A,2008,10"), new IntWritable(1));
		mapReduce.withOutput(new Text("D,1987,7"), new IntWritable(1));
		mapReduce.withOutput(new Text("D,1987,10"), new IntWritable(1));
		mapReduce.withOutput(new Text("D,2008,1"), new IntWritable(1));
		mapReduce.withCounter(DelayCounters.delay_departure, 3);
		mapReduce.withCounter(DelayCounters.delay_arrival, 3);
//		mapReduce.withMultiOutput("departure", new Text("1987,1"), new IntWritable(1));
		
		mapReduce.runTest();
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testMapReduce2() throws IOException {
		/*
		 * year desc
		 * month asc
		 */
		
		mapReduce.setKeyOrderComparator(new DateKeyComparator());
		
		mapReduce.withInput(new LongWritable(), new Text(recode1));
		mapReduce.withInput(new LongWritable(), new Text(recode2));
		mapReduce.withInput(new LongWritable(), new Text(recode3));
		mapReduce.withInput(new LongWritable(), new Text(recode4));
		mapReduce.withOutput(new Text("D,2008,1"), new IntWritable(1));
		mapReduce.withOutput(new Text("D,1987,7"), new IntWritable(1));
		mapReduce.withOutput(new Text("D,1987,10"), new IntWritable(1));
		mapReduce.withOutput(new Text("A,2008,1"), new IntWritable(1));
		mapReduce.withOutput(new Text("A,2008,10"), new IntWritable(1));
		mapReduce.withOutput(new Text("A,1987,10"), new IntWritable(1));
		mapReduce.withCounter(DelayCounters.delay_departure, 3);
		mapReduce.withCounter(DelayCounters.delay_arrival, 3);
//		mapReduce.withMultiOutput("departure", new Text("1987,1"), new IntWritable(1));
		
		mapReduce.runTest();
	}
	


}
