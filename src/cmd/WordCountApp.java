package cmd;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
/*import org.apache.hadoop.mapred.FileOutputFormat;
 import org.apache.hadoop.mapred.TextOutputFormat;           导入这些包就会报错，不是mapred，而是mapreduce
 import org.apache.hadoop.mapred.lib.HashPartitioner;*/
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountApp extends Configured implements Tool{
	static String INPUT_PATH = "";
	static String OUT_PATH = "";

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new WordCountApp(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		INPUT_PATH=args[0];
		OUT_PATH=args[1];
		
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH),new Configuration());
		final Path path = new Path(OUT_PATH);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		
		final Job job = new Job(conf, WordCountApp.class.getSimpleName());
		//打包运行必须执行的秘密方法
		job.setJarByClass(WordCountApp.class);
		// 1.1输入目录在哪里
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		// 指定对输入数据进行格式化处理的类
		job.setInputFormatClass(TextInputFormat.class);
		// 1.2指定自定义的mapper类
		job.setMapperClass(MyMapper.class);
		// 指定map输出的<k,v>类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		// 1.3分区
		job.setPartitionerClass(HashPartitioner.class);		
		job.setNumReduceTasks(1);
		// 1.4排序、分组
		// 1.5归约（可选）
		// 2.2指定自定义的reducer类
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		// 2.3指定输出的路径
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		// 指定输出的格式化类
		job.setOutputFormatClass(TextOutputFormat.class);
		// 把作业提交给jobTracker运行
		job.waitForCompletion(true);
		
		return 0;
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		protected void map(LongWritable k1, Text v1, Context context)
				throws java.io.IOException, InterruptedException {

			final String line = v1.toString();

			final String[] splited = line.split(" ");
			for (String word : splited) {
				context.write(new Text(word), new LongWritable(1));
			}
		}
	}

	/**
	 * KEYIN 即k2 表示行中出现的单词
	 * VALUEIN 即v2 表示行中出现的单词的次数
	 * KEYOUT 即k3 表示文本中出现的不同单词
	 * VALUEOUT 即v3 表示文本中出现的不同单词的总次数
	 * 
	 */
	static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		protected void reduce(Text k2, java.lang.Iterable<LongWritable> v2s,Context ctx)//此处的Context不能加org.apache.hadoop.mapreduce.reducer.
				throws java.io.IOException, InterruptedException {//可能因为没有覆盖原来的reduce方法，没有进行累加
			long times = 0L;
			for (LongWritable count : v2s) {
				times += count.get();
			}
			ctx.write(k2, new LongWritable(times));
		}
	}


}
