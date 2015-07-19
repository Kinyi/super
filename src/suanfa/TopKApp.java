package suanfa;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class TopKApp {
//	static final String INPUT_PATH = "hdfs://hadoop0:9000/in";
//	static final String OUT_PATH = "hdfs://hadoop0:9000/out";
	static final String INPUT_PATH = "hdfs://122.0.67.167:8020/user/18681163341/in";
	static final String OUT_PATH = "hdfs://122.0.67.167:8020/user/18681163341/out";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		final Job job = new Job(conf, TopKApp.class.getSimpleName());
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH),new Configuration());
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
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		// 1.3分区
		job.setPartitionerClass(HashPartitioner.class);		
		job.setNumReduceTasks(1);
		// 1.4排序、分组
		// 1.5归约（可选）
		// 2.2指定自定义的reducer类
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		// 2.3指定输出的路径
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		// 指定输出的格式化类
		job.setOutputFormatClass(TextOutputFormat.class);
		// 把作业提交给jobTracker运行
		job.waitForCompletion(true);
	}

	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
		long max=Long.MIN_VALUE;
		protected void map(LongWritable k1, Text v1, Context context)
				throws java.io.IOException, InterruptedException {
			final long temp = Long.parseLong(v1.toString());
			if(temp>max){
				max=temp;
			}
		}
		@Override
		protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		}
	}

	/**
	 * KEYIN 即k2 表示行中出现的单词
	 * VALUEIN 即v2 表示行中出现的单词的次数
	 * KEYOUT 即k3 表示文本中出现的不同单词
	 * VALUEOUT 即v3 表示文本中出现的不同单词的总次数
	 * 
	 */
	static class MyReducer extends Reducer<LongWritable, NullWritable,LongWritable, NullWritable> {
		long max=Long.MIN_VALUE;
		protected void reduce(LongWritable k2, java.lang.Iterable<NullWritable> v2s,Context ctx)//此处的Context不能加org.apache.hadoop.mapreduce.reducer.可能因为没有覆盖原来的reduce方法，没有进行累加
				throws java.io.IOException, InterruptedException {
			final long k3 = Long.parseLong(k2.toString());
			if(k3>max){
				max=k3;
			}
		}
		@Override
		protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		}
	}

}
