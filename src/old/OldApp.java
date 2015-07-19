package old;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;

/**
 * hadoop版本1.x的包一般是mapreduce
 * hadoop版本0.x的包一般是mapred
 *
 */

public class OldApp {

	static final String INPUT_PATH = "hdfs://chaoren:9000/f2";
	static final String OUT_PATH = "hdfs://chaoren:9000/out";
	static final String URI = "hdfs://chaoren:9000/";
	
	/**
	 * 改动：
	 * 1.不再使用Job，而是使用JobConf
	 * 2.类的包名不再使用mapreduce，而是使用mapred
	 * 3.不再使用job.waitForCompletion(true)提交作业，而是使用JobClient.runJob(job);
	 * 
	 */
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf =new Configuration();
		final JobConf job = new JobConf(conf , OldApp.class);
		final FileSystem fileSystem = FileSystem.get(new java.net.URI(URI), conf);
		final Path path = new Path(OUT_PATH);
		if(fileSystem.exists(path)){
			fileSystem.delete(path, true);
		}
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormat(TextInputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileOutputFormat.setOutputPath(job, path);
		job.setOutputFormat(TextOutputFormat.class);
		
		JobClient.runJob(job);
	}

	/**
	 * 新api:extends Mapper
	 * 老api:extends MapRedcueBase implements Mapper
	 */
	
	static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>{

		@Override
		public void map(LongWritable k1, Text v1,OutputCollector<Text, LongWritable> collector, Reporter reporter)
				throws IOException {
			final String[] split = v1.toString().split(" ");
			for (String word : split) {
				collector.collect(new Text(word), new LongWritable(1));
			}
		}	
	}
	
	static class MyReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>{

		@Override
		public void reduce(Text k2, Iterator<LongWritable> v2s,OutputCollector<Text, LongWritable> collector, Reporter reporter)
				throws IOException {
			long times = 0L;
			//java.util.Iterator类 不能使用迭代，只能用while()循环
			while (v2s.hasNext()) {
				final long count = v2s.next().get();
				times+=count;
			}
			collector.collect(k2, new LongWritable(times));
		}
	}
}
