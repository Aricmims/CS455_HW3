package cs455.hadoop.Q3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class Q3AirlineDataMapper extends Mapper<Object, Text, Text, IntWritable> {

	// mapper value output key: origin airport, value: year flown
	// mapper value output key: destination airport, value: year flown 
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		//obtaining key => airport ID(origin: 8, dest: 12), and value => year (1) 
		
		String [] data = value.toString().split(",");
		String originKey = data[8];
		String destinationKey = data[12];
		String val = data[1];
		
		context.write(new Text(originKey), new IntWritable(Integer.parseInt(val)));
		context.write(new Text(destinationKey), new IntWritable(Integer.parseInt(val)));
		
	}
	
	
	
}
