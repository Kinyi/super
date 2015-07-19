package group;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class GroupApp {

	static final String INPUT_PATH = "hdfs://hadoop0:9000/sort";
	static final String OUT_PATH = "hdfs://hadoop0:9000/out";
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		final Job job = new Job(conf, GroupApp.class.getSimpleName());
		
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		job.setGroupingComparatorClass(MyGroupingComparator.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
	}
	
	/**
	 * �ʣ�Ϊʲô�Զ�����ࣿ
	 * ��ҵ��Ҫ������ǰ��յ�һ�з��飬����NewK2�ıȽϹ�������˲��ܰ��յ�һ�з֡�ֻ���Զ������Ƚ��������յ�һ�н��з���
	 */
	static class MyGroupingComparator implements RawComparator<NewK2>{

		@Override
		public int compare(NewK2 o1, NewK2 o2) {
			return (int)(o1.first-o2.first);
		}

		/**
		 * @param arg0 ��ʾ��һ������Ƚϵ��ֽ�����
		 * @param arg1 ��ʾ��һ������Ƚϵ��ֽ��������ʼλ��
		 * @param arg2 ��ʾ��һ������Ƚϵ��ֽ������ƫ����
		 * 
		 * @param arg3 ��ʾ�ڶ�������Ƚϵ��ֽ�����
		 * @param arg4 ��ʾ�ڶ�������Ƚϵ��ֽ��������ʼλ��
		 * @param arg5 ��ʾ�ڶ�������Ƚϵ��ֽ������ƫ����
		 */
		@Override
		
		//�����ڶ��������з����� s1+8��s2+8
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return WritableComparator.compareBytes(b1, s1, 8, b2, s2, 8);
		}
		
	}
	
	/**
	 * �ʣ�Ϊʲôʵ�ָ��ࣿ
	 * ����Ϊԭ����v2���ܲ������򣬰�ԭ����k2��v2��װ��һ�����У���Ϊ�µ�k2
	 *
	 */
	static class NewK2 implements WritableComparable<NewK2>{
		
		Long first;
		Long second;
		
		public NewK2(){}
		
		public NewK2(long first, long second){
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

		/**
		 * ��k2��������ʱ������ø÷���.
		 * ����һ�в�ͬʱ�����򣻵���һ����ͬʱ���ڶ�������
		 */
		@Override
		public int compareTo(NewK2 o) {			
			if(this.first!=o.first){
				return (int)(this.first - o.first);
			}			
			return (int)(this.second - o.second);
		}
		
		@Override
		public int hashCode() {
			return this.first.hashCode()+this.second.hashCode();
			
		}
		
		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof NewK2)){
				return false;
			}
			NewK2 oK2=(NewK2)obj;
			return (this.first==oK2.first)&&(this.second==oK2.second);
		}
		
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, NewK2 , LongWritable >{
		@Override
		protected void map(LongWritable k1, Text v1,org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			final String[] split = v1.toString().split("\t");
			//final LongWritable k2 = new LongWritable(Long.parseLong(split[0]));
			final NewK2 k2 = new NewK2(Long.parseLong(split[0]), Long.parseLong(split[1]));			
			final LongWritable v2 = new LongWritable(Long.parseLong(split[1]));
			context.write(k2, v2);
		}
	}
	
	/**ȡÿ������ֵ
	 *long max=Long.MIN_VALUE;
			for (LongWritable v2 : v2s) {
				if(v2.get()>max){
					max=v2.get();
				}
			}
	 */
	static class MyReducer extends Reducer<NewK2 , LongWritable , LongWritable , LongWritable >{
		@Override
		protected void reduce(NewK2 k2, Iterable<LongWritable> v2s,Context context)
				throws IOException, InterruptedException {
			//ȡÿ�����Сֵ
			long min=Long.MAX_VALUE;
			for (LongWritable v2 : v2s) {
				if(v2.get()<min){
					min=v2.get();
				}
			}
			context.write(new LongWritable(k2.first),new LongWritable(min));
		}
	}

}