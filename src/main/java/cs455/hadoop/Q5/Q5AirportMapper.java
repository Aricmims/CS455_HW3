package cs455.hadoop.Q5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;

public class Q5AirportMapper extends Mapper <Text, Text,Text,Text>{
	
	// getting the idata(1) and airport name (2)
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] data = value.toString().split(",");
		String idataKey = data[1];
		String name = data[2];
		context.write(new Text(idataKey), new Text(name));
	
	}
}
