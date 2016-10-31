package Cloud.ApacheLog;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

 
public class IpMapper extends MapReduceBase implements
                       	Mapper<LongWritable, Text, Text, Text> {

  String fileName = new String();

  String filename = "";
  String word = "";
  int times = 0;
  int temp = 0;

  public void configure(JobConf job)
  {
	   filename = job.get("map.input.file");
	   times = Integer.parseInt(job.get("times"));
  } 

  public void map(LongWritable fileOffset, Text lineContents,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
        
	String[] array = lineContents.toString().split("[\\p{Punct}\\s]+");
		for(int i=0; i<array.length; i++)
		{
			word = array[i].toLowerCase();
			output.collect(new Text(word),new Text(filename));
			
		}
	}	


  
}
