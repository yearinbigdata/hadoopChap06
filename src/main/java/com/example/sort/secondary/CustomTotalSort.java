package com.example.sort.secondary;

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

import com.example.parser.AirLinePerformanceParser;

/*
 * 시퀀스 && 파티셔너
 */
public class CustomTotalSort extends Configured implements Tool{

	static Log log = LogFactory.getLog(CustomTotalSort.class);
	
	public static void main(String[] args) throws Exception {

		if(args.length == 0){
			ToolRunner.printGenericCommandUsage(System.out);
			
			args = new String[] {"-fs", "hdfs://bigdata01:9000",
									"-jt", "bigdata01:9001"
									};
			
			System.out.println(Arrays.toString(args));
			log.info(Arrays.toString(args));
			
		}
		//분산 캐시 역할
		ToolRunner.run(new CustomTotalSort(), args);
	}

	// mapper는 하둡 데이터 노드들이 필요로 하는 클래스인데 jar 파일로 지정해줘야지 이클립스에서 실행할 수 있다.
	static class DistanceMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		IntWritable outputKey = new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			AirLinePerformanceParser parser = new AirLinePerformanceParser(value);
			
			if(parser.isDistanceAvailable()){
				int distance = parser.getDistance();
				outputKey.set(distance);
				context.write(outputKey, value);
				
			}
		}
	}
	
	static class Desc extends WritableComparator{

		protected Desc() {
			super(IntWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}
	}

	static class MyPartionter extends Partitioner<IntWritable, Text>{

		@Override
		public int getPartition(IntWritable key, Text value, int numPartitions) {
			return key.get() <= 1000 ? 0 : 1;
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception {

		log.info("===============");
		log.info("run(....)......");
		log.info("===============");
		
		
		//job 클라이언트(요청) 작성하기
//		Job job = new Job(getConf(), "SequenceFileCreator...");
		Job job = Job.getInstance(getConf(), "SequenceFileCreator...");
		job.setJarByClass(CustomTotalSort.class);
		
		FileInputFormat.setInputPaths(job, "/home/java/dataexpo/1987_nohead.csv");
		FileInputFormat.addInputPaths(job, "/home/java/dataexpo/1988_nohead.csv");
//		FileInputFormat.setInputPaths(job, "/home/java/dataexpo/");
		job.setInputFormatClass(TextInputFormat.class);
		
		
		/////////////////////////맵리듀스/////////////////////
		job.setMapperClass(DistanceMapper.class);
		
			//노리듀서 맵리듀스프로그램
//		job.setNumReduceTasks(0);

		/* 
		 * 파티셔닝을 통해서 스필 정보를 가져간다: default Hash
		 * 파티션 정보
		 */
		job.setPartitionerClass(MyPartionter.class);
		
		/*
		 * 용량이 너무 크면 리듀서를 여러개로 나누어서 작업하자
		 * 파티셔너: 리듀서를 여러개로 작업할 경우, 매퍼에서 어떤 리듀서에게 작업을 나눌지 결정하는 것
		 * key를 가지고 hash 연산을 수행함
		 */
		job.setNumReduceTasks(2);
		
		job.setSortComparatorClass(Desc.class);
		
			//no reduce이기 때문에 출력 key-value 정의
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		/////////////////////////맵리듀스/////////////////////
		
		//sequence file로 출력: binary 형태
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
//		Path outputDir = new Path("/home/java/dataexpo_seq/1987");
//		Path outputDir = new Path("/home/java/dataexpo_seq/1988");
		Path outputDir = new Path("/home/java/dataexpo_total/1988");
//		Path outputDir = new Path("/home/java/dataexpo_seq/total");
		FileOutputFormat.setOutputPath(job, outputDir);
		
		
		/*
		 * 시퀀스 파일 압축
		 *  압축할 job, 코덱 클래스, 압축 단위 지정
		 */
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);	//압축 단위 설정
		
		
		FileSystem hdfs = FileSystem.get(getConf());
		hdfs.delete(outputDir, true);
		
		job.waitForCompletion(true);
		return 0;
	}

	public int runTest(String[] args) throws Exception {

		FileSystem hdfs = FileSystem.get(getConf());

		// hdfs를 사용하지 않는 것이 Stand Alone 모드: 이클립스에서 Ctrl+F11
		hdfs.create(new Path("/home/java/xxx"));
		hdfs.close();

		return 0;
	}

}
