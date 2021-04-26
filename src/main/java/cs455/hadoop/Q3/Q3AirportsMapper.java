package cs455.hadoop.Q3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Q3AirportsMapper extends Mapper<LongWritable, Text, Text, Text>{

	// getting the airport IDs (1), names, and country (5, will only need US)
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] data = value.toString().split(",");
		String outputKey = data[0];
		String outputValue = data[4];
		context.write(new Text(outputKey), new Text(outputValue));
	
	}
}
