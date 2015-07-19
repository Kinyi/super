package sort_yunshu;

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

public class HiveOrMR {
//把需要排序的字段封装成K2，其他的为V2
	static final String INPUT_PATH = "hdfs://hadoop0:9000/sort";
	static final String OUT_PATH = "hdfs://hadoop0:9000/out";
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		final Job job = new Job(conf, HiveOrMR.class.getSimpleName());
		
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
	}
	
	/**
	 * 问：为什么实现该类？
	 * 答：因为原来的v2不能参与排序，把原来的k2和v2封装到一个类中，作为新的k2
	 *
	 */
	static class NewK2 implements WritableComparable<NewK2>{
		
		Long first;
		Long second;
		Long third;
		
		public NewK2(){}
		
		public NewK2(long first, long second,long third){
			this.first=first;
			this.second=second;
			this.third=third;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(first);
			out.writeLong(second);
			out.writeLong(third);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.first=in.readLong();
			this.second=in.readLong();
			this.third=in.readLong();
		}

		/**
		 * 当k2进行排序时，会调用该方法.
		 * 当第一列不同时，升序；当第一列相同时，第二列升序
		 */
		@Override
		public int compareTo(NewK2 o) {			
			if(this.first!=o.first){
				return (int)(this.first - o.first);
			}			
			//return (int)(this.second - o.second);
			/*else if(this.second!=o.second){
				return (int)(this.second - o.second);
			}
			else {
				return (int)(this.third - o.third);
			}*/
			if(this.first==o.first){
				if(this.second!=o.second){
					return (int)(this.second - o.second);
				}
				else{
					return (int)(this.third - o.third);
				}
			}
			return 0;
		}
		
		@Override
		public int hashCode() {
			return this.first.hashCode()+this.second.hashCode()+this.third.hashCode();
			
		}
		
		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof NewK2)){
				return false;
			}
			NewK2 oK2=(NewK2)obj;
			return (this.first==oK2.first)&&(this.second==oK2.second)&&(this.third==oK2.third);
		}
		
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, NewK2 , Text >{
		@Override
		protected void map(LongWritable k1, Text v1,org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			final String[] split = v1.toString().split(" ");
			//final LongWritable k2 = new LongWritable(Long.parseLong(split[0]));
			final NewK2 k2 = new NewK2(Long.parseLong(split[0]), Long.parseLong(split[1]),Long.parseLong(split[4]));			
			//final LongWritable v2 = new LongWritable(Long.parseLong(split[1]));
			String temp = split[2]+" "+split[3]+" "+split[5]+" "+split[6]+" "+split[7]+" "+split[8];
			//LongWritable v2 = new LongWritable(Long.parseLong(temp));
			Text v2 = new Text(temp);
			//System.out.println(k2.first.toString()+" "+k2.second.toString()+" "+k2.third.toString());
			context.write(k2, v2);
		}
	}
	
	static class MyReducer extends Reducer<NewK2 , Text , Text , Text >{
		@Override
		protected void reduce(NewK2 k2, Iterable<Text> v2s,Context context)
				throws IOException, InterruptedException {
			String tmp = k2.first.toString()+" "+k2.second.toString()+" "+k2.third.toString();
			//context.write(new LongWritable(k2.first), new LongWritable(k2.second));
			for (Text v2 : v2s) {
				context.write(new Text(tmp), v2);
			}
		}
	}

}
