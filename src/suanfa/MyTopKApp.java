package suanfa;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


public class MyTopKApp {
	static final String INPUT_PATH = "hdfs://hadoop0:9000/in";
	static final String OUT_PATH = "hdfs://hadoop0:9000/out";
	static final String URI = "hdfs://hadoop0:9000/";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		final Job job = new Job(conf, MyTopKApp.class.getSimpleName());
		final FileSystem fileSystem = FileSystem.get(new URI(URI),new Configuration());
		final Path path = new Path(OUT_PATH);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		// 1.1输入目录在哪里
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		// 指定对输入数据进行格式化处理的类
		job.setInputFormatClass(TextInputFormat.class);
		// 1.2指定自定义的mapper类
		job.setMapperClass(MyMapper.class);
		// 指定map输出的<k,v>类型
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		// 1.3分区
		job.setPartitionerClass(HashPartitioner.class);		
		job.setNumReduceTasks(1);
		// 1.4排序、分组
		// 1.5归约（可选）
		// 2.2指定自定义的reducer类
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		// 2.3指定输出的路径
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		// 指定输出的格式化类
		job.setOutputFormatClass(TextOutputFormat.class);
		// 把作业提交给jobTracker运行
		job.waitForCompletion(true);
		
	}

	public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable>{
		@Override
		protected void map(LongWritable k1,Text v1,	Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			IntWritable k2 = new IntWritable(Integer.parseInt(v1.toString()));
			context.write(k2, NullWritable.get());
		}
	}
	
	public static class MyReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable>{
		ArrayList<Integer> container = new ArrayList<Integer>();
		
		@Override
		protected void reduce(IntWritable k2,Iterable<NullWritable> v2s,
				Reducer<IntWritable, NullWritable, IntWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			container.add(k2.get());
		}
		
		@Override
		protected void cleanup(Reducer<IntWritable, NullWritable, IntWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
//			Collections.sort(container, Comparator<Integer>);
			//升序取前5个
			for (int i = 0; i < 5; i++) {
				context.write(new IntWritable(container.get(i)), NullWritable.get());
			}
			//降序取后5个
			/*for (int i = container.toArray().length; i > container.toArray().length - 5 ; i--) {
				context.write(new IntWritable(container.get(i-1)), NullWritable.get());
			}*/
		}
	}
}
