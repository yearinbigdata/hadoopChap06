package com.example.delay.mos;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.example.parser.AirLinePerformanceParser;
import com.example.parser.DelayCounters;
import com.example.sort.DateKey;

/*
 *  workType을 입력 받지 않고, arrive / departure 모두 출력하자
 *  
 *  1. workType 필드가 사라진다. —> if문 없이 arrive/departure 모두 동작해야 한다.
 *  2. 출력 결과에 연도+월 이라는 key가 구분되지 않는다. —> reduce에서 집계할 때 구분할 수 있도록
 *  
 *  
 */
public class DelayCountMapper extends Mapper<LongWritable, Text, DateKey, IntWritable> {

	final IntWritable one = new IntWritable(1);

	DateKey outputKey = new DateKey();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		AirLinePerformanceParser parser = new AirLinePerformanceParser(value);

		/*
		 * 출발 지연
		 */
		if (parser.isDepatureDelayAvailable()) {
			if (parser.getDepartureDelayTime() > 0) {

//				outputKey.set("D," + parser.getYear() + "," + parser.getMonth()); // key와 group을 변경함
				outputKey.setYear("D," + parser.getYear());
				outputKey.setMonth(parser.getMonth());
				context.write(outputKey, one);

				context.getCounter(DelayCounters.delay_departure).increment(1);

			} else if (parser.getDepartureDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_departure).increment(1);
			} else {
				context.getCounter(DelayCounters.early_departure).increment(1);
			}
		} else {
			context.getCounter(DelayCounters.not_available_departure).increment(1);
		}

		/*
		 * 도착 지연
		 */
		if (parser.isArriveDelayAvailable()) {
			if (parser.getArriveDelayTime() > 0) {
//				outputKey.set("A," + parser.getYear() + "," + parser.getMonth());
				outputKey.setYear("A," + parser.getYear());
				outputKey.setMonth(parser.getMonth());
				context.write(outputKey, one);
				context.getCounter(DelayCounters.delay_arrival).increment(1);
			} else if (parser.getArriveDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_arrival).increment(1);
			} else {
				context.getCounter(DelayCounters.early_arrival).increment(1);
			}
		} else {
			context.getCounter(DelayCounters.not_available_departure).increment(1);

		}

	}
}