package hive_dong;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class transform {
	
	static class myMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void map(LongWritable k1, Text v1,org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			final String[] split = v1.toString().split(" ");
			String ip = split[0];
			String time = split[3].replace("[", "");
			String path = split[6];
			String v2 = "ip="+ip + "|" + "time="+time + "|" + "path="+path;
			context.write(k1, new Text(v2));
		}
	}
	
	static class myReducer extends Reducer<LongWritable, Text, NullWritable, Text>{
		@Override
		protected void reduce(LongWritable k2, Iterable<Text> v2s,Context context)
				throws IOException, InterruptedException {
			for (Text v2 : v2s) {
				context.write(NullWritable.get(), v2);
			}
		}
	}
	
	private static final String INPUT_PATH = "hdfs://hadoop0:9000/hbase_data/dongxicheng.org.log.1";
	private static final String OUT_PATH = "hdfs://hadoop0:9000/hive_data/webLogs";
	private static final String URI = "hdfs://hadoop0:9000/hive_data/";

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		final Job job = new Job(conf,transform.class.getSimpleName());
		
		final FileSystem fileSystem = FileSystem.get(new java.net.URI(URI), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(myMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		job.setReducerClass(myReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
	}

}
