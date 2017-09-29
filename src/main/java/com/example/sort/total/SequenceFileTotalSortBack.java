package com.example.sort.total;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tools.ant.types.selectors.ExtendSelector;

import com.example.parser.AirLinePerformanceParser;

public class SequenceFileTotalSortBack extends Configured implements Tool{

	static Log log = LogFactory.getLog(SequenceFileTotalSort.class);
	
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
		ToolRunner.run(new SequenceFileTotalSort(), args);
	}
	

	
	@Override
	public int run(String[] args) throws Exception {
		log.info("###########");
		log.info("run(...)...");
		log.info("###########");
		
		Job job = Job.getInstance(getConf(), "SequenceFileCreator..."); // = Job job = new Job(conf, jobName);
		job.setJarByClass(SequenceFileTotalSort.class);
		
		FileInputFormat.setInputPaths(job, "/home/java/dataexpo/1987_nohead.csv");
		FileInputFormat.addInputPaths(job, "/home/java/dataexpo/1988_nohead.csv");
//		FileInputFormat.setInputPaths(job, "/home/java/dataexpo");		//total
		job.setInputFormatClass(TextInputFormat.class);		//외부와 맵리듀스 프로그램의 창구역할
		
		////////////////////////////////////////////////////////
//		job.setMapperClass(DistanceMapper.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setNumReduceTasks(2);		//no Reduce
//		job.setSortComparatorClass(Desc.class);	//desc sort
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		////////////////////////////////////////////////////////
		
		//파티션 위치 지정(알려줌)
		Path partitionFile = new Path("/home/java/dataexpo_partitions");
		partitionFile = partitionFile.makeQualified(partitionFile.getFileSystem(getConf()));
		TotalOrderPartitioner.setPartitionFile(getConf(), partitionFile);
		//샘플링 방법 지시
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(0.1, 1000);
		InputSampler.writePartitionFile(job, sampler);
		
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
