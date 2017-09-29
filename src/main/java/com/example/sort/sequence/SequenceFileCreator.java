package com.example.sort.sequence;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tools.ant.types.selectors.ExtendSelector;

import com.example.parser.AirLinePerformanceParser;

public class SequenceFileCreator extends Configured implements Tool{

	static Log log = LogFactory.getLog(SequenceFileCreator.class);
	
	public static void main(String[] args) throws Exception {
		if (args.length == 0){
			ToolRunner.printGenericCommandUsage(System.out);
			
			//ToolRunner를 통해서 명령어 치지 않고 간단하게 hdfs에 올릴 수 있게 셋팅
			args = new String[] {
					"-fs", "hdfs://bigdata01:9000", "-jt", "bigdata01:9001",			//jobTracker
			};		//nameNode 구체화. hdfs에 만들어지도록!
			System.out.println(Arrays.toString(args));
			log.info(Arrays.toString(args));
		}
		ToolRunner.run(new SequenceFileCreator(), args);
	}
				//운항거리 뽑아내서 시퀀스 파일 만들 때, 레코드의 키를 distance로 할 것. -> 운항거리별 항공데이터 뽑아낼 수 있음
	static class DistanceMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		
		IntWritable outputKey = new IntWritable();	//distanceAvailable에서 효율적으로 돌기 위해 필드에서 outputKey 생성
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			AirLinePerformanceParser parser = new AirLinePerformanceParser(value);
			
			if (parser.isDistanceAvailable()){	//유효한 데이터만 사용하도록 필터링
				int distance = parser.getDistance();
				outputKey.set(distance);					//distance 뽑아냄
				context.write(outputKey, value);
			}
			
			
			
			
		}
	}
	
	static class Desc extends WritableComparator {

		protected Desc() {
			super(IntWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}
		
	}
	
	static class MyPartitioner extends Partitioner<IntWritable, Text>{

		@Override
		public int getPartition(IntWritable key, Text value, int numPartitions) {
			
			return key.get() <=1000 ? 0 : 1;	//키값이 1000이하면 0번 리듀스에 보내고, 1000보다 크면 1번 리듀스로 보낸다
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		log.info("###########");
		log.info("run(...)...");
		log.info("###########");
		
		Job job = Job.getInstance(getConf(), "SequenceFileCreator..."); // = Job job = new Job(conf, jobName);
		job.setJarByClass(SequenceFileCreator.class);
		
		FileInputFormat.setInputPaths(job, "/home/java/dataexpo/1987_nohead.csv");
		FileInputFormat.addInputPaths(job, "/home/java/dataexpo/1988_nohead.csv");
//		FileInputFormat.setInputPaths(job, "/home/java/dataexpo");		//total
		job.setInputFormatClass(TextInputFormat.class);		//외부와 맵리듀스 프로그램의 창구역할
		
		job.setMapperClass(DistanceMapper.class);
		
		job.setNumReduceTasks(0);		//no Reduce
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		////////////////////////////////////////////////////////
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);	//binary형식으로 출력하기 위해
//		Path outputDir = new Path("/home/java/dataexpo_seq/1987");
		Path outputDir = new Path("/home/java/dataexpo_seq/1988");
//		Path outputDir = new Path("/home/java/dataexpo_seq/total");
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//압축 설정: 블록 단위로 압축
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		
		FileSystem hdfs = FileSystem.get(getConf());
		hdfs.delete(outputDir, true);
				
		
		job.waitForCompletion(true);
		return 0;
	}

}
