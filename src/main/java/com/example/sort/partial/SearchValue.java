package com.example.sort.partial;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * hdfs에서 검색하는 프로그램 : 204페이지
 */
public class SearchValue extends Configured implements Tool {

	static Log log = LogFactory.getLog(SearchValue.class);
	
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			ToolRunner.printGenericCommandUsage(System.out);

			args = new String[] { 
					"-fs", "hdfs://bigdata01:9000", "-jt", "bigdata01:9001", 
				};
			System.out.println(Arrays.toString(args));
			log.info(Arrays.toString(args));
		}
		ToolRunner.run(new SearchValue(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		log.info("###########");
		log.info("run(...)...");
		log.info("###########");

		//배열을 리턴함 [STREAM] = 통의 수를 리턴
		Reader[] reader = MapFileOutputFormat.getReaders(FileSystem.get(getConf()), 
											new Path("/home/java/dataexpo_map/1988"), 
											getConf());

		log.info("reader.length = " + reader.length);
		
		//100마일 거리의 운항 데이터를 갖고 있는 녀석을 찾아보자!
		IntWritable findKey = new IntWritable();
		findKey.set(10);
		Text value = new Text();
		
		//bucket을 찾는 방법 1
		for(int i=0;i<reader.length;i++){
			if(reader[i].get(findKey, value) != null){
				System.out.println("i = "+ i);
			}
		}

		//bucket을 찾는 방법 2
		int bucket = new HashPartitioner<IntWritable, Text>().getPartition(findKey, value, reader.length);
		System.out.println("bucket = " + bucket);
		
		/*
		 * 맴파일을 이용한 검색 방법 : 맵파일의 인덱스에서 먼저 내가 원하는 것 찾고 그 중에서 진짜 내가 원하는 걸 찾는다.
		 * [중요한 전제] 키가 그루핑되어서 파티션 되어 있기 때문에 key값이 같은 것은 같은 파일 안에 있다.
		 * 1. 통이 어디에 있는가? 
		 * 2. 통에서 key를 갖고 찾아보쟈!
		 */
		
		Reader r = reader[bucket];

		if(r.get(findKey, value) == null){
			System.out.println("not found key =" + findKey.get());
			log.info("not found...key =" + findKey.get());
			
			//찾은게 없으면 프로그램 종료
			System.exit(-1);
		}
		int cnt=0;
		System.out.println(value);
		cnt++;
		
		IntWritable nextKey  = new IntWritable();
		
		//현재의 통에서 같은 key를 갖고 있는 데이터를 모두 출력해보자
		while(r.next(nextKey, value)){
			if(findKey.equals(nextKey)){
				System.out.println(value);
				cnt++;
			} else
				break;
		}
		System.out.println(findKey.get() + ", cnt = " + cnt);
		return 0;
	}
}