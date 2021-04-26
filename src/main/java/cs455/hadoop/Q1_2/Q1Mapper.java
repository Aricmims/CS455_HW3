package cs455.hadoop.Q1_2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
//import java.util.Scanner;


/*
 * Mapper to grab the data to answer the question What is the best (and worst) time of day,
 *  day of week, and time of year to fly */
public class Q1Mapper extends Mapper<LongWritable, Text,Text,DoubleWritable>{
	// the mapper takes one line of the CSV at a time and grabs what I need 
	
	// Mapper value output: time of year (2), day of week(4), and time of day (14) and value delay (15)
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// obtaining key => date/time info (MarchWed10AM) and value => delay times 
		String[] data = value.toString().split(",");
		String outputKey = data[1] + " " + data[3] + " " + data[13];
		String outputValue = data[14];
		
		// checking if the data is valid AKA NOT THE FIRST LINE!! 
		if(isInvalid(data[1])) {
			return;
		}
		// making sure we have the right stuff
		if(!(outputValue.equals("") || outputValue.equals("\"DEP_DELAY\""))) {
			context.write(new Text(outputKey), new DoubleWritable(Double.parseDouble(outputValue)));
		}
	}
	
	public boolean isInvalid(String s) {
		return s.equals("MONTH");
	}
	
}