package hmbbs;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class cleaner extends Configured implements Tool{
	
	static class logParser{
		
		public String[] parse(String line) throws ParseException{
			String ip = this.parseIp(line);
			String date = this.parseDate(line);
			String url = this.parseUrl(line);
			return new String[]{ip,date,url};
		}
		public String parseIp(String line){
			final String ip = line.split("- -")[0].trim();
			return ip;
		}
		public String parseDate(String line) throws ParseException{
			final int first = line.indexOf("[");
			final int last = line.lastIndexOf("+0800]");
			final String time = line.substring(first+1, last).trim();
			final SimpleDateFormat parseDate = new SimpleDateFormat("dd/MMMM/yyyy:HH:mm:ss",Locale.ENGLISH);
			final Date parse = parseDate.parse(time);
			final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
			final String date = simpleDateFormat.format(parse);
			return date;
		}
		public String parseUrl(String line){
			final String[] split = line.split(" ");
			return split[6];
		}
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void map(LongWritable k1, Text v1,org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			final logParser record = new logParser();
			Text v2 = new Text();
			String[] parsed= null;
			try {
				parsed = record.parse(v1.toString());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(parsed[2].startsWith("/static/") || parsed[2].startsWith("/uc_server")){
				return;
			}
			v2.set(parsed[0]+"\t"+parsed[1]+"\t"+parsed[2]);
			context.write(k1, v2);
		}
	}
	
	static class MyReducer extends Reducer<LongWritable, Text, NullWritable, Text>{
		@Override
		protected void reduce(LongWritable k2, Iterable<Text> v2s,Context context)
				throws IOException, InterruptedException {
			for (Text v2 : v2s) {
				context.write(NullWritable.get(), v2);
			}
		}
	}


	public static void main(String[] args) throws Exception {
		ToolRunner.run(new cleaner(), args);
	}

	private static String INPUT_PATH = null;
	private static String OUTPUT_PATH = null;
	@Override
	public int run(String[] args) throws Exception {
		
		INPUT_PATH=args[0];
		OUTPUT_PATH=args[1];
		
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUTPUT_PATH))){
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		
		final Job job = new Job(conf,cleaner.class.getSimpleName());
		job.setJarByClass(cleaner.class);
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.waitForCompletion(true);
		
		return 0;
	}

}
