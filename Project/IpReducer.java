package Cloud.ApacheLog;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import java.util.HashMap; 
import java.util.*; 


public class IpReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
{	

Map<String,Integer> myMap = new HashMap<String,Integer>();

  public void reduce(Text ip, Iterator<Text> counts,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
	double count;
	int counter1 = 0;

	ArrayList <String> strings = new ArrayList <String>();
     
     Text firstelement = new Text();
     firstelement = counts.next();
     if(myMap.get(firstelement.toString()) == null) 
     {	
       myMap.put(firstelement.toString(), 1);
     } 
     else
     {
        myMap.put(firstelement.toString(), myMap.get(firstelement.toString())+1); 
     }

	
	 while(counts.hasNext())
	 {	

		System.out.println("Input:" + firstelement.toString());
	     if(myMap.get(firstelement.toString()) == null)
	     {	
		myMap.put(firstelement.toString(), 1);
	     }
	     else
	     {
		int counter = myMap.get(firstelement.toString());
		counter+=1;
	 	myMap.put(firstelement.toString(), counter);
	     }
		firstelement = counts.next();
    	 }  
     Iterator<Map.Entry<String, Integer>> iterator = myMap.entrySet().iterator();
     while(iterator.hasNext()){
	   Map.Entry<String, Integer> entry = iterator.next();
	   System.out.println("IP: " + ip + " Key: " + entry.getKey() + " Value: " + entry.getValue());
	   strings.add(entry.getKey());
	   StringBuilder sb = new StringBuilder();
	   sb.append("");
	   sb.append(entry.getValue());	
	   strings.add(sb.toString());
	   iterator.remove(); 
     }
     StringBuilder sb = new StringBuilder();
     for (String s : strings)
     {
	    sb.append(s);
	    sb.append("\t");
     }
     output.collect(new Text(ip),new Text(sb.toString()));
		
    	
  }
}
