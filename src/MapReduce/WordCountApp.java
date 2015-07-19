package MapReduce;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
/*import org.apache.hadoop.mapred.FileOutputFormat;
 import org.apache.hadoop.mapred.TextOutputFormat;           ������Щ���ͻᱨ��������mapred������mapreduce
 import org.apache.hadoop.mapred.lib.HashPartitioner;*/
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordCountApp {
	static final String INPUT_PATH = "hdfs://hadoop0:9000/hello";
	static final String OUT_PATH = "hdfs://hadoop0:9000/out";
	static final String URI = "hdfs://hadoop0:9000/";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		final Job job = new Job(conf, WordCountApp.class.getSimpleName());
		final FileSystem fileSystem = FileSystem.get(new URI(URI),new Configuration());
		final Path path = new Path(OUT_PATH);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		// 1.1����Ŀ¼������
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		// ָ�����������ݽ��и�ʽ����������
		job.setInputFormatClass(TextInputFormat.class);
		// 1.2ָ���Զ����mapper��
		job.setMapperClass(MyMapper.class);
		// ָ��map�����<k,v>����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		// 1.3����
		job.setPartitionerClass(HashPartitioner.class);		
		job.setNumReduceTasks(1);
		// 1.4���򡢷���
		// 1.5��Լ����ѡ��
		// 2.2ָ���Զ����reducer��
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		// 2.3ָ�������·��
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		// ָ������ĸ�ʽ����
		job.setOutputFormatClass(TextOutputFormat.class);
		// ����ҵ�ύ��jobTracker����
		job.waitForCompletion(true);
	}

	static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		protected void map(LongWritable k1, Text v1, Context context)
				throws java.io.IOException, InterruptedException {

			final String line = v1.toString();

			final String[] splited = line.split("\t");
			for (String word : splited) {
				context.write(new Text(word), new LongWritable(1));
			}
		}
	}

	/**
	 * KEYIN ��k2 ��ʾ���г��ֵĵ���
	 * VALUEIN ��v2 ��ʾ���г��ֵĵ��ʵĴ���
	 * KEYOUT ��k3 ��ʾ�ı��г��ֵĲ�ͬ����
	 * VALUEOUT ��v3 ��ʾ�ı��г��ֵĲ�ͬ���ʵ��ܴ���
	 * 
	 */
	static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		protected void reduce(Text k2, java.lang.Iterable<LongWritable> v2s,Context ctx)//�˴���Context���ܼ�org.apache.hadoop.mapreduce.reducer.
				throws java.io.IOException, InterruptedException {//������Ϊû�и���ԭ����reduce������û�н����ۼ�
			long times = 0L;
			for (LongWritable count : v2s) {
				times += count.get();
			}
			ctx.write(k2, new LongWritable(times));
		}
	}

}