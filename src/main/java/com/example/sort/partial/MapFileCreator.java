package com.example.sort.partial;

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
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapFileCreator extends Configured implements Tool {

	static Log log = LogFactory.getLog(MapFileCreator.class);
	
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			ToolRunner.printGenericCommandUsage(System.out);

			args = new String[] { 
					"-fs", "hdfs://bigdata01:9000", "-jt", "bigdata01:9001", 
				};
			System.out.println(Arrays.toString(args));
			log.info(Arrays.toString(args));
		}
		ToolRunner.run(new MapFileCreator(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		log.info("###########");
		log.info("run(...)...");
		log.info("###########");

		JobConf job = new JobConf(getConf());
		job.setJarByClass(MapFileCreator.class);

		FileInputFormat.addInputPaths(job, "/home/java/dataexpo_seq/1988");
//		FileInputFormat.setInputPaths(job, "/home/java/dataexpo_seq");
		job.setInputFormat(SequenceFileInputFormat.class);

		/*
		 * 매퍼를 통과하고, 리듀서 들어가기 전에 정렬 과정이 있기 때문에
		 *  아웃풋 파일 각각 정렬이 되어있다. --> 부분 정렬
		 *  결과 전체를 정렬하기 위해서는 --> total sort 필요
		 */

		job.setNumReduceTasks(3);						
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		/////////////////////////////////////
		
		/*
		 * Map File은 개념적으로 java의 hashmap과 동일하다.
		 * file에 저장			| 메모리에 저장
		 * map file은 1.2.1때 mapreduce 패키지에 구현되지 않았다. --> mapred 패키지 사용해야 한다.
		 */
		
		//map file로 출력: map 형식으로 저장
		job.setOutputFormat(MapFileOutputFormat.class);
		Path outputDir = new Path("/home/java/dataexpo_map/1988");
//		Path outputDir = new Path("/home/java/dataexpo_seq/total");
		FileOutputFormat.setOutputPath(job, outputDir);

		/*
		 * 맵파일도 시퀀스로 절절히 두고 검색할 때는 관계형 디비처럼 인덱싱함
		 * 출력: sequenceFile로 압축 --> 필수는 아닌거.. 퍼포먼스 & 효율 고려 ..
		 */
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

		FileSystem hdfs = FileSystem.get(getConf());
		hdfs.delete(outputDir, true);

		JobClient.runJob(job);
		
		hdfs.delete(new Path(outputDir.toString()+"/_SUCCESS"), true);
		hdfs.delete(new Path(outputDir.toString()+"/_logs"), true);
		
		hdfs.close();
		
		return 0;
	}

}

/*
 * 검색 방법 : 맵파일의 인덱스에서 먼저 내가 원하는 것 찾고 그 중에서
 * 진짜 내가 원하는 걸 찾는다. 
 */
