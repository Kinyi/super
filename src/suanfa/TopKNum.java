package suanfa;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//利用MapReduce求最大值海量数据中的K个数 (网上找的,没看懂) 
public class TopKNum extends Configured implements Tool {
	public static class MapClass extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {
		public static final int K = 100;
		private int[] top = new int[K];

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] str = value.toString().split(",", -2);
			try {// 对于非数字字符我们忽略掉
				int temp = Integer.parseInt(str[8]);
				add(temp);
			} catch (NumberFormatException e) {
			}
		}

		private void add(int temp) {// 实现插入
			if (temp > top[0]) {
				top[0] = temp;
				int i = 0;
				for (; i < 99 && temp > top[i + 1]; i++) {
					top[i] = top[i + 1];
				}
				top[i] = temp;
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (int i = 0; i < 100; i++) {
				context.write(new IntWritable(top[i]), new IntWritable(top[i]));
			}
		}
	}

	public static class Reduce extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public static final int K = 100;
		private int[] top = new int[K];

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			for (IntWritable val : values) {
				add(val.get());
			}
		}

		private void add(int temp) {// 实现插入if(temp>top[0]){
			top[0] = temp;
			int i = 0;
			for (; i < 99 && temp > top[i + 1]; i++) {
				top[i] = top[i + 1];
			}
			top[i] = temp;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			for (int i = 0; i < 100; i++) {
				context.write(new IntWritable(top[i]), new IntWritable(top[i]));
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "TopKNum");
		job.setJarByClass(TopKNum.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TopKNum(), args);
		System.exit(res);
	}
}
