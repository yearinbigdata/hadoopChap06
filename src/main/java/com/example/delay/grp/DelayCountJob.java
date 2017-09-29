package com.example.delay.grp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.example.sort.DateKey;
import com.example.sort.DateKeyComparator;
import com.example.sort.DateKeyGroupComparator;

/*
 * workType을 입력 받지 않고, arrive / departure 모두 출력하자
 */

public class DelayCountJob extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DelayCountJob(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {  
    
    Configuration conf = getConf();
    
    Job job = new Job(conf, "DelayCount year/month Sort");
    
    job.setJarByClass(DelayCountJob.class);
    
    /*
     * 1. Input 단계
     */
    
    FileInputFormat.setInputPaths(job, "/home/java/dataexpo/1987_nohead.csv");
    FileInputFormat.addInputPaths(job, "/home/java/dataexpo/1988_nohead.csv");
    FileInputFormat.addInputPaths(job, "/home/java/dataexpo/1989_nohead.csv");

//    FileInputFormat.addInputPaths(job, "/home/java/dataexpo");
    job.setInputFormatClass(TextInputFormat.class);
    
    /*
     * 2. Map 단계
     */
    job.setMapperClass(DelayCountMapper.class);
    job.setMapOutputKeyClass(DateKey.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    /*
     * 3. sort 단계
     */
//    job.setSortComparatorClass(DateKeyComparator.class);
//    job.setSortComparatorClass(WritableComparator.class);
    job.setGroupingComparatorClass(DateKeyGroupComparator.class);
    
    /*
     * 4. Reduce 단계
     */
    job.setReducerClass(DelayCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    /*
     * 5. Output 단계
     */
    job.setOutputFormatClass(TextOutputFormat.class);
//    Path outputDir = new Path("/home/java/dataexpo_out/1987");
//    Path outputDir = new Path("/home/java/dataexpo_out/1988");
    Path outputDir = new Path("/home/java/dataexpo_out/1989");

//    Path outputDir = new Path("/home/java/dataexpo_out/total");

    FileOutputFormat.setOutputPath(job, outputDir);
    
    MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, Text.class, IntWritable.class);
    MultipleOutputs.addNamedOutput(job, "arrive", TextOutputFormat.class, Text.class, IntWritable.class);
    
    FileSystem hdfs = FileSystem.get(conf);
    hdfs.delete(outputDir, true);
    
    return job.waitForCompletion(true) ? 0 : -1;          //return은 정수 값을 리턴하므로 :: 잡이 성공적으로 끝나면 0으로 리턴, 실패하면 -1로 리턴
    
  }

}