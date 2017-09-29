package com.example.sort.total;


import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SequenceFileTotalSort extends Configured implements Tool {

	static Log log = LogFactory.getLog(SequenceFileTotalSort.class);
	
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			ToolRunner.printGenericCommandUsage(System.out);

			args = new String[] { 
					"-fs", "hdfs://bigdata01:9000", "-jt", "bigdata01:9001", 
				};
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

		JobConf job = new JobConf(getConf());
		job.setJarByClass(SequenceFileTotalSort.class);

		FileInputFormat.addInputPaths(job, "/home/java/dataexpo_seq/1988");
//		FileInputFormat.setInputPaths(job, "/home/java/dataexpo");
		job.setInputFormat(SequenceFileInputFormat.class);

		////////////////////////////////////////////////////////////
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		job.setNumReduceTasks(4); // no reduce
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		//////////////////////////////////////////////////////////
		
		job.setOutputFormat(SequenceFileOutputFormat.class);
		Path outputDir = new Path("/home/java/dataexpo_totalorder/1988");
//		Path outputDir = new Path("/home/java/dataexpo_totalorder/total");
		FileOutputFormat.setOutputPath(job, outputDir);

		Path inputDir = new Path("/home/java/dataexpo_partitions");
		inputDir = inputDir.makeQualified(inputDir.getFileSystem(getConf()));
		TotalOrderPartitioner.setPartitionFile(job, inputDir);
		
		//샘플러. 0.1퍼센트의 확률로 1000개의 레코드를 뽑아낸다
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(0.1, 1000);
		InputSampler.writePartitionFile(job, sampler);
		
		//출력 압축
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

		FileSystem hdfs = FileSystem.get(getConf());
		hdfs.delete(outputDir, true);

		JobClient.runJob(job);
		return 0;
	}

}
