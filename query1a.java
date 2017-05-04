import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class query1a 
{
	public static class MyClass extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String parts[] = value.toString().split("\t");
			if(parts[4].contains("DATA ENGINEER"))
			context.write(new Text(parts[7]),new IntWritable(1));
		}
	}
	
	public static class Myreducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException
		{
			int count = 0;
			for (IntWritable val:values)
			{
				count += val.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	
	public static void main(String args[])throws Exception
	{
Configuration conf=new Configuration();
Job job=Job.getInstance(conf,"bvr1");
job.setJarByClass(query1a.class);
job.setMapperClass(MyClass.class);
job.setReducerClass(Myreducer.class);

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);

FileInputFormat.addInputPath(job,new Path(args[0]));
FileSystem.get(conf).delete(new Path(args[1]),true);
FileOutputFormat.setOutputPath(job,new Path(args[1]));
System.exit(job.waitForCompletion(true)?0:1);
		
		
	}	
}
