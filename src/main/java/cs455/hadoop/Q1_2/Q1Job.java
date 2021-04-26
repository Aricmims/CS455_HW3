package cs455.hadoop.Q1_2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class Q1Job {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			
			Configuration conf = new Configuration();
			
			// name the job 
			Job job = Job.getInstance(conf, "Question 1");
			
			// configure the job 
			job.setJarByClass(Q1Job.class);
			job.setMapperClass(Q1Mapper.class);
			job.setReducerClass(Q1Reducer.class);
			
			
			// input/output paths 
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setNumReduceTasks(1);
			
			//outputs from the mapper
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			
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