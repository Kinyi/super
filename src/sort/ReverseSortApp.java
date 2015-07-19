package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class ReverseSortApp {

	static final String INPUT_PATH = "hdfs://chaoren:9000/sort";
	static final String OUT_PATH = "hdfs://chaoren:9000/out";
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		final Job job = new Job(conf, ReverseSortApp.class.getSimpleName());
		
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(NewK2.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
	}

	static class MyMapper extends Mapper<LongWritable, Text, NewK2, LongWritable>{
		@Override
		protected void map(LongWritable k1, Text v1,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			final String[] split = v1.toString().split("\t");
			final NewK2 k2 = new NewK2(Long.parseLong(split[0]),Long.parseLong(split[1]));
			context.write(k2, k1);
		}
	}
	
	static class MyReducer extends Reducer<NewK2, LongWritable, LongWritable, LongWritable>{
		@Override
		protected void reduce(NewK2 k2, Iterable<LongWritable> v2s,Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(k2.first),new LongWritable(k2.second));
		}
	}
	
	static class NewK2 implements WritableComparable<NewK2>{
		
		Long first;
		Long second;		
		
		public NewK2(){}
		
		public NewK2(long first,long second){
			this.first=first;
			this.second=second;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(first);
			out.writeLong(second);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.first=in.readLong();
			this.second=in.readLong();
		}

		@Override
		public int compareTo(NewK2 o) {
			if(this.first!=o.first){
				return (int)(o.first-this.first);//只修改了此处
			}
			return (int)(this.second-o.second);
		}
		
		@Override
		public int hashCode() {
			return this.first.hashCode()+this.second.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			if(obj instanceof NewK2){
				return false;
			}
			NewK2 oK2=(NewK2)obj;
			return (this.first==oK2.first)&&(this.second==oK2.second);
		}
		
	}
}
