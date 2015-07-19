package hbase_dong;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//��hbase�е������ݣ��������hdfs��
//��ͬ����practice.java��ͬһ����ҵ
public class access_time {
	
	public final static String IP = "175.44.19.36";
	public final static String OUT_PATH = "hdfs://hadoop0:9000/hbase_data/allWeb_time";
	public final static String URI = "hdfs://hadoop0:9000/";
	
	static class WebTableMapper extends org.apache.hadoop.hbase.mapreduce.TableMapper<Text, Text>{
		@Override
		//key��Ӧ����hbase���е�rowkey
		protected void map(ImmutableBytesWritable key, Result value,org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			//final String[] split = key.toString().split("-");
			final String[] split = Bytes.toString(key.get()).split("-");
			String ip = split[0];
			String[] all_time = split[1].split("/");
			final String[] time = all_time[2].split(":");
			String hour = time[1];
			String min = time[2];
			String second = time[3];
			for (KeyValue kv : value.list()) {
				if(IP.equals(ip) && ((Integer.parseInt(hour)<12)||(hour=="12" && min=="00" && second=="00"))){
					context.write(new Text(ip), new Text(Bytes.toString(kv.getValue())));
				}
			}
		}
	}
	
	static class WebTableReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,Context context)
				throws IOException, InterruptedException {
			String url = "";
			for (Text v2 : v2s) {
				url+=v2.toString()+"\n";
				//context.write(k2, v2);
			}
			context.write(k2, new Text(url));
		}
	}
	
	public static void main(String[] args) throws Exception {
		final Configuration configuration = new Configuration();
		// ����zookeeper
		configuration.set("hbase.zookeeper.quorum", "hadoop0");
		// ����hbase������
		configuration.set(TableInputFormat.INPUT_TABLE, "access_log");
		// ����ֵ�Ĵ󣬷�ֹhbase��ʱ�˳�
		configuration.set("dfs.socket.timeout", "180000");

		final FileSystem fileSystem = FileSystem.get(new URI(URI),new Configuration());
		final Path path = new Path(OUT_PATH);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		
		final Job job = new Job(configuration, "access");
		
		//job.setJarByClass(access.class);
		
		job.setMapperClass(WebTableMapper.class);
		job.setReducerClass(WebTableReducer.class);
		// ����map�������������reduce���������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TableInputFormat.class);
		// �����������·�����������������ʽ����
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop0:9000/hbase_data/allWeb_time"));

		job.waitForCompletion(true);
		
		//��ӡ����Ļ��
	/*	URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		final URL url = new URL("hdfs://hadoop0:9000/hbase_data/allWeb_time/part-r-00000");
		final InputStream in = url.openStream();
		IOUtils.copyBytes(in, System.out, configuration, true);*/
	}
}
