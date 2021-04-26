package cs455.hadoop.Q5;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;



//Which airports experience the most delay due to late aircraft?

public class Q5AirlineDataMapper extends Mapper<Text,Text,Text,Text> {

	// mapper needs to grab the origin (8, will match w/ Airport data), and 
	// late aircraft delay (32)
	
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] data = value.toString().split(",");
		String originKey = data[8];
		String delay = data[32];
		context.write(new Text(originKey), new Text(delay));
	
	}
	
}
