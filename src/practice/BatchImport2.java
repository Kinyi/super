package practice;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class BatchImport2 {

	static final String PATH = "hdfs://hadoop0:9000/wlan";
	
	public static void main(String[] args) throws Exception {
		final Configuration configuration = new Configuration();
		configuration.set("hbase.zookeeper.quorum", "hadoop0");
		configuration.set("hbase.socket.timeout", "180000");
		configuration.set(TableOutputFormat.OUTPUT_TABLE, "wlan");
		
		final FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop0:9000/"), configuration);
		if(fileSystem.exists(new Path(PATH))){
			fileSystem.delete(new Path(PATH), true);
		}
		
		final Job job = new Job(configuration, BatchImport2.class.getSimpleName());
		FileInputFormat.setInputPaths(job, "hdfs://hadoop0:9000/input");
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(BatchImportMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		job.setReducerClass(BatchImportReducer.class);
		
		job.setOutputFormatClass(TableOutputFormat.class);
		
		job.waitForCompletion(true);
	}
	
	static class BatchImportMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void map(LongWritable k1, Text v1,org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			final String[] split = v1.toString().split("\t");
			try {
				final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
				final Date date = new Date(Long.parseLong(split[0].trim()));
				final String format = simpleDateFormat.format(date);
				final Text v2 = new Text(split[1]+":"+format+"\t"+v1);
				context.write(k1, v2);
			} catch (NumberFormatException e) {
				final Counter counter = context.getCounter("BatchImport", "Error");
				counter.increment(1L);
				System.out.println("³ö´íÁË "+split[0]+" "+e.getMessage());
			}
		}
	}
	
	static class BatchImportReducer extends TableReducer<LongWritable, Text, NullWritable>{
		@Override
		protected void reduce(LongWritable k2, Iterable<Text> v2s,Context context)
				throws IOException, InterruptedException {
			for (Text text : v2s) {
				final String[] split = text.toString().split("\t");
				final Put put = new Put(split[0].getBytes());
				put.add("cf".getBytes(), "date".getBytes(), split[1].getBytes());
				context.write(NullWritable.get(), put);
			}
		}
	}

}
