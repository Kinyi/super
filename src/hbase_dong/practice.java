package hbase_dong;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class practice {
	//�����ݰ�Ҫ���hdfs�е��뵽hbase��
	//��ͬ����access.java��ͬһ����ҵ
	static class ImportMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable k1, Text v1,org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			final String[] split = v1.toString().split(" ");
			String ip = split[0];
			String time = split[3];
			String url = split[6];
			String rowKey = ip+"-"+time;
			context.write(new Text(rowKey), new Text(url));
		}
	}

	static class ImportReducer extends TableReducer<Text, Text, NullWritable>{
		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,Context context)
				throws IOException, InterruptedException {
			for (Text v2 : v2s) {
				final Put put = new Put(Bytes.toBytes(k2.toString()));
				put.add(Bytes.toBytes("info"), Bytes.toBytes("url"), Bytes.toBytes(v2.toString()));
				context.write(NullWritable.get(), put);
			}
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		final Configuration configuration = new Configuration();
		// ����zookeeper
		configuration.set("hbase.zookeeper.quorum", "hadoop0");
		// ����hbase������
		configuration.set(TableOutputFormat.OUTPUT_TABLE, "access_log");
		// ����ֵ�Ĵ󣬷�ֹhbase��ʱ�˳�
		configuration.set("dfs.socket.timeout", "180000");

		final Job job = new Job(configuration, "practice");

		job.setMapperClass(ImportMapper.class);
		job.setReducerClass(ImportReducer.class);
		// ����map�������������reduce���������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		// �����������·�����������������ʽ����
		job.setOutputFormatClass(TableOutputFormat.class);

		FileInputFormat.setInputPaths(job, "hdfs://hadoop0:9000/hbase_data/");

		job.waitForCompletion(true);
	}

}
