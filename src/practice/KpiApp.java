package practice;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class KpiApp {

	static final String INPUT_PATH = "hdfs://chaoren:9000/wlan";
	static final String OUT_PATH = "hdfs://chaoren:9000/out";
	static final String URI = "hdfs://chaoren:9000/";
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf=new Configuration();
		final Job job = new Job(conf, KpiApp.class.getSimpleName());
		final FileSystem fileSystem = FileSystem.get(new java.net.URI(URI), conf);
		final Path path = new Path(OUT_PATH);
		if(fileSystem.exists(path)){
			fileSystem.delete(path, true);
		}
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(KpiWritable.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KpiWritable.class);
		
		FileOutputFormat.setOutputPath(job, path);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, KpiWritable>{
		@Override
		protected void map(LongWritable k1, Text v1,org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			final String[] split = v1.toString().split("\t");
			final Text k2 = new Text(split[1]);
			final KpiWritable v2 = new KpiWritable(split[6],split[7],split[8],split[9]);
			context.write(k2, v2);
		}
	}

	static class MyReducer extends Reducer<Text, KpiWritable, Text, KpiWritable>{
		@Override
		protected void reduce(Text k2, Iterable<KpiWritable> v2s,Context context)
				throws IOException, InterruptedException {
			long upPackNum=0L;
			long downPackNum=0L;
			long upPayLoad=0L;
			long downPayLoad=0L;
			for (KpiWritable kpiWritable : v2s) {
				upPackNum+=kpiWritable.upPackNum;
				downPackNum+=kpiWritable.downPackNum;
				upPayLoad+=kpiWritable.upPayLoad;
				downPayLoad+=kpiWritable.downPayLoad;
			}
			final KpiWritable v3 = new KpiWritable(upPackNum+"",downPackNum+"",upPayLoad+"",downPayLoad+"");
			context.write(k2, v3);
		}
	}
	
}

class KpiWritable implements Writable{
	
	long upPackNum=0L;
	long downPackNum=0L;
	long upPayLoad=0L;
	long downPayLoad=0L;
	
	public KpiWritable(){}
	
	public KpiWritable(String upPackNum,String downPackNum,String upPayLoad,String downPayLoad){
		this.upPackNum=Long.parseLong(upPackNum);
		this.downPackNum=Long.parseLong(downPackNum);
		this.upPayLoad=Long.parseLong(upPayLoad);
		this.downPayLoad=Long.parseLong(downPayLoad);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upPackNum);
		out.writeLong(downPackNum);
		out.writeLong(upPayLoad);
		out.writeLong(downPayLoad);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.upPackNum=in.readLong();
		this.downPackNum=in.readLong();
		this.upPayLoad=in.readLong();
		this.downPayLoad=in.readLong();
	}
	
	@Override
	public String toString() {
		return upPackNum+"\t"+downPackNum+"\t"+upPayLoad+"\t"+downPayLoad;
	}
}
