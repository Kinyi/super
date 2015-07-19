package practice;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordCountApp {

	static final String INPUT_PATH = "hdfs://chaoren:9000/f2";
	static final String OUT_PATH = "hdfs://chaoren:9000/out";
	static final String URI = "hdfs://chaoren:9000/";
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		final Job job = new Job(conf , WordCountApp.class.getSimpleName());
		final FileSystem fileSystem = FileSystem.get(new java.net.URI(URI), conf);
		final Path path = new Path(OUT_PATH);
		if(fileSystem.exists(path)){
			fileSystem.delete(path, true);
		}
		//System.out.println(WordCountApp.class.getSimpleName());
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, path);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);

	}
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		protected void map(LongWritable k1, Text v1,Context context)
				throws IOException, InterruptedException {
			final String[] split = v1.toString().split(" ");
			for (String word : split) {
				context.write(new Text(word), new LongWritable(1L));
			}
		}
	}
	
	static class MyReducer extends Reducer<Text, LongWritable, Text , LongWritable>{
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,Context context)
				throws IOException, InterruptedException {
			long times=0L;
			for (LongWritable count : v2s) {
				times+=count.get();
			}
			context.write(k2, new LongWritable(times));
		}
	}

}
