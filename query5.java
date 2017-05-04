import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class query5 extends Configured implements Tool
{
	public static class Myclass extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String parts[] = value.toString().split("\t");
			if(parts[7].startsWith("2"))
			{
			context.write(new Text(parts[4]), value);
		}
		}
	}
	public static class mypart extends Partitioner<Text,Text>
	{
		public int getPartition(Text key,Text value,int numReduceTasks)
		{
			String parts[] = value.toString().split("\t");
			if(numReduceTasks == 0)
			{
				return 0;
			}
			if(parts[7].equals("2011"))
			{
				return 0;
			}
			if(parts[7].equals("2012"))
			{
				return 1;
			}
			if(parts[7].equals("2013"))
			{
				return 2;
			}
			if(parts[7].equals("2014"))
			{
				return 3;
			}
			if(parts[7].equals("2015"))
			{
				return 4;
			}
			if(parts[7].equals("2016"))
			{
				return 5;
			}else
				return 6;
		}
	}
	public static class Myreducer extends Reducer<Text,Text,NullWritable,Text>
	{
		private TreeMap<Integer,Text>  ftt = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  ftt1 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  ftt2 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  ftt3 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  ftt4 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  ftt5 = new TreeMap<Integer,Text>();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
		{
			int count=0;
			String year ="";
			String myval = key.toString();
			for(Text val :values)
			{
				String parts[] = val.toString().split("\t");
				count++;
				 year = parts[7];
			}
			 myval = myval+','+ year+','+count;
			if(year.equals("2011"))                        //year 2011 details
			{
				ftt.put(count,new Text(myval));
				if(ftt.size()>10)
				{
					ftt.remove(ftt.firstKey());
				}
			}
			if(year.equals("2012"))                         // year 2012 details
			{
				ftt1.put(count,new Text(myval));
				if(ftt1.size()>10)
				{
					ftt1.remove(ftt1.firstKey());
				}
			}
			if(year.equals("2013"))                           // year 2013 details
			{
				ftt2.put(count,new Text(myval));
				if(ftt2.size()>10)
				{
					ftt2.remove(ftt2.firstKey());
				}
			}
			if(year.equals("2014"))                           //year 2014 details
			{
				ftt3.put(count, new Text(myval));
				if(ftt3.size()>10)
				{
					ftt3.remove(ftt3.firstKey());
				}
			}
			if(year.equals("2015"))                             //year 2015 details
			{
				ftt4.put(count,new Text(myval));
				if(ftt4.size()>10)
				{
					ftt4.remove(ftt4.firstKey());
				}
			}
			if(year.equals("2016"))                               //year 2016 details
			{
				ftt5.put(count,new Text(myval));
				if(ftt5.size()>10)
				{
					ftt5.remove(ftt5.firstKey());
				}
			}
			}
		public void cleanup(Context context) throws IOException,InterruptedException
		{
			for(Text val:ftt.descendingMap().values())
			{
				context.write(NullWritable.get(), val);
			}
			for(Text bl:ftt1.descendingMap().values())
			{
				context.write(NullWritable.get(),bl);
			}
			for(Text bl:ftt2.descendingMap().values())
			{
				context.write(NullWritable.get(),bl);
			}for(Text bl:ftt3.descendingMap().values())
			{
				context.write(NullWritable.get(),bl);
			}for(Text bl:ftt4.descendingMap().values())
			{
				context.write(NullWritable.get(),bl);
			}for(Text bl:ftt5.descendingMap().values())
			{
				context.write(NullWritable.get(),bl);
			}
		}
	}
	public  int run(String []args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job name");
		job.setJarByClass(query5.class);
		job.setPartitionerClass(mypart.class);
		job.setMapperClass(Myclass.class);
		job.setReducerClass(Myreducer.class);
		job.setNumReduceTasks(7);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}
	public static void main(String []ar) throws Exception
	{
		ToolRunner.run(new Configuration(), new query5(), ar);
	}

}
