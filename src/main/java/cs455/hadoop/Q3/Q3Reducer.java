package cs455.hadoop.Q3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.TreeMap;

/*
 * What are the major hubs (busiest airports) in continental U.S.?
 *  Please list the top 10. Has there been a change over the 
 *  21-year period covered by this dataset?
*/
public class Q3Reducer extends Reducer <Text, Text, Text, Text> {

	// Mapping the US airports w/ the total number of flights (the number of values in the mapper value) 
	private TreeMap<String, Integer> busiestAirports = new TreeMap<>();
	// Airport, Year, and Flights to find the trend
	//private Map<>
}
