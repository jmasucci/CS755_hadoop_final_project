package Cloud.ApacheLog;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.TextInputFormat;

public class Runner extends Configured implements Tool {
	public int run(String[] args) throws Exception {
/*
		JobConf job = new JobConf(Runner.class);
		//JobConf job = job.getInstance(getConf());
		job.setJobName("wordcount");
		//job.setJarByClass(Runner.class);
		
		
		/* Field separator for reducer output*/
		//job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");
		
/*		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(IpMapper.class);
		job.setCombinerClass(IpReducer.class);
		job.setReducerClass(IpReducer.class);

		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		Path inputFilePath = new Path(args[0]);
                MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class);
		Path outputFilePath = new Path(args[2]);

		/* This line is to accept input recursively */
		//FileInputFormat.setInputDirRecursive(job, true);

		//FileInputFormat.addInputPath(job, inputFilePath);
		//job.set("map.input.file", args[0]);
		//FileOutputFormat.setOutputPath(job, outputFilePath);


		/* Delete output filepath if already exists */
		//FileSystem fs = FileSystem.newInstance(getConf());
/*
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
*/
/*
		//return job.waitForCompletion(true) ? 0 : 1;
		JobClient.runJob(job);
		return 0;
*/
		return 0;
	}
	
	public static void main(String[] args) throws Exception {

		int i=0;
		JobConf job = new JobConf(Runner.class);
		job.setJobName("wordcount");
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(IpMapper.class);
		job.setCombinerClass(IpReducer.class);
		job.setReducerClass(IpReducer.class);
		
		Path inputFilePath = new Path(args[0]);
		for(i=0; i<args.length-1; i++)
		{
                job.set("map.input.file", args[i]);
		MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class, IpMapper.class);
		job.set("times", Integer.toString(args.length));
		}
		
		Path outputFilePath = new Path(args[i]);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		job.setNumReduceTasks(1);
		JobClient.runJob(job);
                
}
}
