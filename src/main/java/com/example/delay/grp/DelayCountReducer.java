package com.example.delay.grp;

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
		int bMonth = key.getMonth();
		int sum=0;		//month sum
		int totalSum=0;	
		for (IntWritable v : values) {
			if(bMonth != key.getMonth()){
				outputKey.set(key.getYear() + "," + bMonth);
				outValue.set(sum);
				context.write(outputKey, outValue);
				
				String[] keys = key.getYear().split(",");
				if (keys[0].equals("D"))
					mos.write("departure", new Text(keys[1] +  "," + bMonth), outValue); // departure-r-00000
				
				if (keys[0].equals("A"))
					mos.write("arrive", new Text(keys[1] + "," + bMonth), outValue);
				
				bMonth = key.getMonth();
				sum=0;
			}
			sum += v.get();	
			totalSum += v.get();
		}
		
		//마지막 것 써지지 않기때문에 집계 함께 하기 위해서 추가
		outputKey.set(key.getYear() + "," + bMonth);
		outValue.set(sum);
		context.write(outputKey, outValue);
		
		String[] keys = key.getYear().split(",");
		if (keys[0].equals("D"))
			mos.write("departure", new Text(keys[1] +  "," + bMonth), outValue); // departure-r-00000
		
		if (keys[0].equals("A"))
			mos.write("arrive", new Text(keys[1] + "," + bMonth), outValue); // arrive-r-00000
		
		//totalSum 출력
		outputKey.set("total : ");
		outValue.set(totalSum);
		context.write(outputKey, outValue);
		
		keys = key.getYear().split(",");
		if (keys[0].equals("D"))
			mos.write("departure", new Text("total : "), outValue); // departure-r-00000
		if (keys[0].equals("A"))
		mos.write("arrive", new Text("total : "), outValue); // arrive-r-00000
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
