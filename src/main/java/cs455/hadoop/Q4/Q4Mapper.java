package cs455.hadoop.Q4;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/*
 * Q4: Carriers w/ most delays (will need total number of delayed flights
 * total minutes lost to delays and who has the highest avg delay
 * */
 
public class Q4Mapper extends Mapper<Text,Text,Text,Text> {

	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		// obtaining 
	}
}
