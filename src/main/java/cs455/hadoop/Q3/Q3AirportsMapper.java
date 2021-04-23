package cs455.hadoop.Q3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Q3AirportsMapper extends Mapper<Object, Text, Text, Text>{

	// getting the airport IDs (1), names, and country (5, will only need US)
	
	protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
		String[] data = value.toString().split(",");
		String outputKey = data[1];
		String outputValue = data[5];
		context.write(new Text(outputKey), new Text(outputValue));
	
	}
}
