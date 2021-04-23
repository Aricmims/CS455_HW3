package cs455.hadoop.Q4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class Q4Job {
	
	public static void main(String args[]) {
		try {
		Configuration conf = new Configuration();
		
		// name the job 
		Job job = Job.getInstance(conf, "Question 3");
		
		// configure the job 
		job.setJarByClass(Q4Job.class);
		job.setMapperClass(Q4Mapper.class);
		job.setCombinerClass(Q4Reducer.class);
		job.setReducerClass(Q4Reducer.class);
		
		
		// input/output paths 
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(10);
		
		//outputs from the reducer 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Blocking until job is complete 
		System.exit(job.waitForCompletion(true) ? 0:1);
		
		
	} catch (Exception e) {
		
		System.err.println(e.getMessage());
		
	}
		
	}
}
