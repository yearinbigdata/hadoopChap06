package com.example.delay.mos;

import java.io.IOException;

import javax.xml.transform.OutputKeys;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.example.sort.DateKey;

public class DelayCountReducer extends Reducer<DateKey, IntWritable, Text, IntWritable> {

	IntWritable outValue = new IntWritable();
	Text outputKey = new Text();
	
	MultipleOutputs<Text, IntWritable> mos;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<>(context);
	}
	
	@Override
	protected void reduce(DateKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		/*
		 * 출발지연
		 */
		int sum=0;
		for (IntWritable v : values) {
			sum += v.get();
		}
		outValue.set(sum);
		outputKey.set(key.getYear()+","+key.getMonth());
		
		context.write(outputKey, outValue);
		
		String[] keys = key.getYear().split(",");
		if (keys[0].equals("D"))
			mos.write("departure", new Text(keys[1] +  "," + key.getMonth()), outValue); // departure-r-00000
		
		if (keys[0].equals("A"))
			mos.write("arrive", new Text(keys[1] + "," + key.getMonth()), outValue); // arrive-r-00000
		
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
