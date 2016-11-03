package join;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InMemoryJoin {
	  public static class Map extends Mapper<LongWritable, Text, Text, Text> {

	    private static Text user1, user2;
	    private static HashMap<String, String> dataMap = new HashMap<>();
	    StringBuilder friendDetails = new StringBuilder("[");
	    public void setup(Context context) {
	      Configuration conf = context.getConfiguration();
	     
	      String proPath = conf.get("userdata");
	      Path pt=new Path("hdfs://cshadoop1"+proPath);
	      FileSystem fs;
	      
	      
	      try {
	        fs = FileSystem.get(new Configuration());
	        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	        String line;
	        line=br.readLine();
	        while (line != null){
	        	 	String splitBy[] =line.split(",");
	                String friend=splitBy[0];
	                if(splitBy.length == 10){
	                String res = splitBy[1]+":"+ splitBy[9];
	                dataMap.put(friend.trim(),res.trim());
	                }
	                line=br.readLine();
	               
	       }
	      } catch (IOException e) {
	        System.err.println("Error reading the data file");
	        e.printStackTrace();
	        System.exit(2);
	      }
	      
	    }

	    public void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException {
	          String line = value.toString();
	          StringTokenizer str = new StringTokenizer(line, "\t");
	          String fromFriend = str.nextToken();
	          String listNameAge = str.nextToken();
	          HashMap<String, String> friendHmap = new HashMap<>();
	          str = new StringTokenizer(listNameAge.substring(1, listNameAge.length() - 2), ",");
	          String friend = new String();
	          while (str.hasMoreTokens()) {
	            friend = str.nextToken().trim();
	            friendHmap.put(friend, dataMap.get(friend));
	          }
	          context.write(new Text(fromFriend), new Text(friendHmap.toString()));
	        }
	      }

	  public static class Reduce extends Reducer<Text, Text, Text, Text> {
	    public void reduce(Text key, Iterable<Text> values, Context context)
	        throws IOException, InterruptedException {
	    }
	  }

	  // Driver program
	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args)
	        .getRemainingArgs();
	    // get all args
	    if (otherArgs.length != 6) {
	      System.err.println("Usage: WordCount <user1> <user2> <networkdata> <output> <userdata> <output>");
	      System.exit(2);
	    }

	    conf.set("user1", otherArgs[0]);
	    conf.set("user2", otherArgs[1]);
	    
		
	    // create a job with name "wordcount"
	    Job job = new Job(conf, "mutualfriends");
	    
	    // set friend 1 and friend 2 as parameter

	    job.setJarByClass(InMemoryJoin.class);
	    job.setMapperClass(mutualFriendPair.Map.class);
	    
	    job.setReducerClass(mutualFriendPair.Reduce.class);
	    // uncomment the following line to add the Combiner
	    // job.setCombinerClass(Reduce.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);

	    // set output key type
	    job.setOutputKeyClass(Text.class);
	    // set output value type
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
	    Path p=new Path(otherArgs[3]);
	    FileOutputFormat.setOutputPath(job, p);
	    int code = job.waitForCompletion(true)?0:1;
	    Configuration conf2 = new Configuration();
	    conf2.set("userdata", otherArgs[4]);
	    Job job2 = new Job(conf2,"friendInfo");
	    job2.setJarByClass(InMemoryJoin.class);
	    job2.setMapperClass(Map.class);
	    job2.setNumReduceTasks(0);
	   // job2.setReducerClass(.Reduce.class);
	    // uncomment the following line to add the Combiner
	    // job.setCombinerClass(Reduce.class);
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(Text.class);

	    // set output key type
	    job2.setOutputKeyClass(Text.class);
	    // set output value type
	    job2.setOutputValueClass(Text.class);
	    // set the HDFS path of the input data
	    FileInputFormat.addInputPath(job2, p);
	    // set the HDFS path for the output
	    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5]));
	    // Wait till job completion
	    code = job2.waitForCompletion(true) ? 0 : 1;
	    System.exit(code);
	  }
	}