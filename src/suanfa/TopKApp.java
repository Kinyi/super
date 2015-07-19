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
		// 1.1����Ŀ¼������
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		// ָ�����������ݽ��и�ʽ���������
		job.setInputFormatClass(TextInputFormat.class);
		// 1.2ָ���Զ����mapper��
		job.setMapperClass(MyMapper.class);
		// ָ��map�����<k,v>����
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		// 1.3����
		job.setPartitionerClass(HashPartitioner.class);		
		job.setNumReduceTasks(1);
		// 1.4���򡢷���
		// 1.5��Լ����ѡ��
		// 2.2ָ���Զ����reducer��
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		// 2.3ָ�������·��
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		// ָ������ĸ�ʽ����
		job.setOutputFormatClass(TextOutputFormat.class);
		// ����ҵ�ύ��jobTracker����
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
	 * KEYIN ��k2 ��ʾ���г��ֵĵ���
	 * VALUEIN ��v2 ��ʾ���г��ֵĵ��ʵĴ���
	 * KEYOUT ��k3 ��ʾ�ı��г��ֵĲ�ͬ����
	 * VALUEOUT ��v3 ��ʾ�ı��г��ֵĲ�ͬ���ʵ��ܴ���
	 * 
	 */
	static class MyReducer extends Reducer<LongWritable, NullWritable,LongWritable, NullWritable> {
		long max=Long.MIN_VALUE;
		protected void reduce(LongWritable k2, java.lang.Iterable<NullWritable> v2s,Context ctx)//�˴���Context���ܼ�org.apache.hadoop.mapreduce.reducer.������Ϊû�и���ԭ����reduce������û�н����ۼ�
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
