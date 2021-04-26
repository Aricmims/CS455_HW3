package cs455.hadoop.Q3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class Q3AirlineDataMapper extends Mapper<LongWritable, Text, Text, Text> {

	// mapper value output key: origin airport, value: year flown
	// mapper value output key: destination airport, value: year flown 
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//obtaining key => airport ID(origin: 8, dest: 12), and value => year (1) 
		
		String [] data = value.toString().split(",");
		String originKey = data[7];
		String destinationKey = data[11];
		String val = data[0];
		
		context.write(new Text(originKey), new Text(val));
		context.write(new Text(destinationKey), new Text(val));
		
	}
	
	
	
}
