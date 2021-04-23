package cs455.hadoop.Q3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Q3Job {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			
			Configuration conf = new Configuration();
			
			// name the job 
			Job job = Job.getInstance(conf, "Question 3");
			
			// configure the job 
			job.setJarByClass(Q3Job.class);
			job.setMapperClass(Q3AirlineDataMapper.class);
			job.setMapperClass(Q3AirportsMapper.class);
			job.setCombinerClass(Q3Reducer.class);
			job.setReducerClass(Q3Reducer.class);
			
			
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
