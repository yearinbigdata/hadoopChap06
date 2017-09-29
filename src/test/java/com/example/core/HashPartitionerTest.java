package com.example.core;

import static org.junit.Assert.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.junit.Test;

public class HashPartitionerTest {

	@Test
	public void test() {
		HashPartitioner<IntWritable, Text> partition = new HashPartitioner<>();
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		for(int i=0; i<20; i++){
			key.set(i);
			
			int pnum = partition.getPartition(key, value, 2);
			System.out.println("i=" + i + ", pnum = " + pnum);
			
		}
		
	}

}
