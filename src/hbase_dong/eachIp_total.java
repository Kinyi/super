package hbase_dong;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//��hbase�е������ݣ��������hbase��
//��ͬ����practice.java��ͬһ����ҵ
public class eachIp_total {
	
	public final static String IP = "175.44.19.36";
	
	static class WebTableMapper extends org.apache.hadoop.hbase.mapreduce.TableMapper<Text, Text>{
		@Override
		//key��Ӧ����hbase���е�rowkey
		protected void map(ImmutableBytesWritable key, Result value,org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			//final String[] split = key.toString().split("-");
			final String[] split = Bytes.toString(key.get()).split("-");
			String ip = split[0];
			for (KeyValue kv : value.list()) {
					context.write(new Text(ip), new Text(Bytes.toString(kv.getValue())));
			}
		}
	}
	
	static class WebTableReducer extends TableReducer<Text, Text, NullWritable>{
		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,Context context)
				throws IOException, InterruptedException {
			long count = 0;
			for (Text v2 : v2s) {
				count+=1;
			}
			final Put put = new Put(Bytes.toBytes(k2.toString()));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("total"), Bytes.toBytes(Long.toString(count)));
			context.write(NullWritable.get(), put);
		}
	}
	
	/*static class WebTableReducer extends Reducer<Text, Text, Text, LongWritable>{
		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,Context context)
				throws IOException, InterruptedException {
			long count = 0;
			for (Text v2 : v2s) {
				count+=1;
			}
			context.write(k2, new LongWritable(count));
		}
	}*/
	
	public static void main(String[] args) throws Exception {
		final Configuration configuration = new Configuration();
		// ����zookeeper
		configuration.set("hbase.zookeeper.quorum", "hadoop0");
		// ����hbase������
		configuration.set(TableInputFormat.INPUT_TABLE, "access_log");
		configuration.set(TableOutputFormat.OUTPUT_TABLE, "total_access");
		// ����ֵ�Ĵ󣬷�ֹhbase��ʱ�˳�
		configuration.set("dfs.socket.timeout", "180000");

		final Job job = new Job(configuration, "access");
		
		//job.setJarByClass(access.class);
		
		job.setMapperClass(WebTableMapper.class);
		job.setReducerClass(WebTableReducer.class);
		// ����map�������������reduce���������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		//job.setOutputValueClass(Put.class);

		job.setInputFormatClass(TableInputFormat.class);
		// �����������·�����������������ʽ����
		job.setOutputFormatClass(TableOutputFormat.class);

		job.waitForCompletion(true);
		
		//��ӡ����Ļ��
	/*	URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		final URL url = new URL("hdfs://hadoop0:9000/hbase_data/allWeb/part-r-00000");
		final InputStream in = url.openStream();
		IOUtils.copyBytes(in, System.out, configuration, true);*/
	}
}
