package cs455.hadoop.Q1_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/*
 * Reducer to calculate answer to Q1 and Q2. Will go through the mapper output, 
 * calculate the average for each data then put it in the set so it will be automatically
 *  sorted and duplicates will be delt with 
 * */

// could be an int writable 
public class Q1Reducer extends Reducer<Text,Text,Text,Text>{
	
	// everything should be added in order (hopefully)
	private TreeMap<Integer, String> bestTravelTimes = new TreeMap<>();

	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		// get the average for values 
		int avg = calculateAverage(values);
		
		// add the readable key and the average value to the map 
		addToMap(key, avg);
	}
	
	protected void cleanup(Context context) {
		// answering the question
		// outputing the answer (MAY NEED TO MOVE TO A SEPERATE METHOD TO MAKE COMPARISON CORRECT!!)
		
		try {
			context.write(new Text("Question 1:"), new Text());
			Entry<Integer, String> best = bestTravelTimes.firstEntry();
			context.write(new Text("The best time to fly is "), new Text(best.getValue()));
			context.write(new Text("Question 2:"), new Text());
			Entry<Integer, String> worst = bestTravelTimes.lastEntry();
			context.write(new Text("The worst time to fly is "), new Text(worst.getValue()));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
	}
	
	// Method to calculate the average delay at a given time on a day of the month 
	public int calculateAverage(Iterable <IntWritable> values) {
		// loop through values, convert them to ints, calculate average and return it 
		int total= 0, count = 0;
		
		for(IntWritable val: values) {
			total += val.get();
			count++;
		}
		
		return total/count;
	}
	
	// Method put the key into readable format and put them into the map  
	public void addToMap(Text key, int avgDelay) {
		// decode the key to get the string 
		
		// the string to be build 
		String s = "";
		// should be something like [1, 1 0521] (month, day of week, flight departure time) 
		String[] splitKey = key.toString().split("$");
		s.concat(findMonth(splitKey[0]));
		s.concat(findDay(splitKey[1]));
		s.concat(findTime(splitKey[2]));
		
		
		bestTravelTimes.put(avgDelay, s);
		
		
	}
	
	public String findMonth(String s) {
		switch(s){
		case "1": 
			return "January ";
		case "2":
			return("Febuary ");
		case "3": 
			return("March ");
		case "4":
			return("April ");
		case "5":
			return("May ");
		case "6":
			return("June ");
		case "7":
			return("July ");
		case "8":
			return("August ");
		case "9":
			return("September ");
		case "10":
			return("October ");
		case "11":
			return("November ");
		case "12":
			return("December ");
		default: return "MONTH IS FUCKED";
		
		}
	}
	
	public String findDay(String s) {
		switch(s) {
		case "1":
			return "Monday ";
		case "2":
			return "Tuesday ";
		case "3":
			return "Wednesday ";
		case "4":
			return "Thursday ";
		case "5": 
			return "Friday ";
		case "6": 
			return "Saturday ";
		case "7": 
			return "Sunday ";
		default: return "DAY IS FUCKED";
		}
		
	}
		public String findTime(String s) {
			return s.substring(0,2) + ":" + s.substring(2);
		}
		
		
}