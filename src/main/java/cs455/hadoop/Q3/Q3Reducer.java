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
	

	
	protected void reduce(Text key, Text values, Context context) throws IOException, InterruptedException{
		// getting the total number of flights per airport  and putting them together
		
		int totalFlights = totalFlights(values);
		
		// adding it to the total flights
		addToMap(key, totalFlights);
	}
	
	protected void cleanup(Context context) {
		try {
			context.write(new Text("Busiest airports"), new Text(busiestAirports.toString()));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public int totalFlights(Text values) {
		
		String[] flights = values.toString().split("");
		return flights.length;
		
//		works for an iterable NOT a text value
//		 int total = 0;
//		
//		for(Text val:values) {
//			total++;
//		}
//		
//		return total;
	}
	
	public void addToMap(Text key, int total) {
		// find the key in the airports, increment the total if its in the list, add with a one if its not
	}
	
}
