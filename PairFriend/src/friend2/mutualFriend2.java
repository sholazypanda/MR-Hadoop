package friend2;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class mutualFriend2 {
  public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    private static Text user1, user2;

    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      user1 = new Text(conf.get("user1"));
      user2 = new Text(conf.get("user2"));
    }

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
		String[] mydata = value.toString().split("\t");
		String result="";
		if(mydata.length == 2){
		int fromFriend = Integer.parseInt(mydata[0]);
		String users = mydata[0];
		if(users.equals(user1.toString()) || users.equals(user2.toString()) ){
		//Set<Integer> set = new HashSet<Integer>();
		ArrayList<Integer> list = new ArrayList<Integer>();
		String[] toFriendArray = mydata[1].split(",");
		for (int i=0;i<toFriendArray.length;i++) {	
			//set.add(Integer.parseInt(toFriendArray[i]));
			list.add(Integer.parseInt(toFriendArray[i]));
		}
		for (Integer ele:list ) {
			if(fromFriend < ele){
				result = fromFriend+","+ele;
			}
			else{
				result = ele+","+fromFriend;
			}
			if (result.contains(user1.toString())
		              && result.contains(user2.toString())){
			context.write(new Text(result),new Text(removeFriend(list,result).toString()));
			}
		}
		}
		}
    }
  }
	public static List<Integer> removeFriend(List<Integer> list,String result){
		
		
		String splitString[] = result.split(",");
		if(list.contains(splitString[1])){
			list.remove(splitString[1]);
		}
		return list;
	}

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
    	List<String> resultSet = new ArrayList<String>();
		List<String> a = new ArrayList<String>();
		HashMap<String,Integer> hm = new HashMap<String,Integer>();
		for(Text val:values){
			
			String strSplit[] = val.toString().split(",");
			for(int i=0;i<strSplit.length;i++){
				a.add(strSplit[i].trim());
			}
		}
		if(a.size() > 0){
			
		
		for( int i=0;i<a.size();i++){
			if(hm.containsKey(a.get(i))){
				int count = hm.get(a.get(i));
				hm.put(a.get(i), count+1);
			}
			else{
				hm.put(a.get(i),1);
			}
		}
		
		for(String keyMap: hm.keySet()){
			if(hm.get(keyMap)==2){
				resultSet.add(keyMap);
			}
		}
		//a.retainAll(b);

		Text resultValue = new Text(resultSet.toString());
		 // create a pair <keyword, number of
		context.write(key,resultValue);
		}
    }
  }

  // Driver program
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    // get all args
    if (otherArgs.length != 4) {
      System.err.println("Usage: WordCount <in> <out> <friend1> <friend2>");
      System.exit(2);
    }

    conf.set("user1", args[0]);
    conf.set("user2", args[1]);

    // create a job with name "wordcount"
    Job job = new Job(conf,"friendsPair");

    // set friend 1 and friend 2 as parameter

    job.setJarByClass(mutualFriend2.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    // uncomment the following line to add the Combiner
    // job.setCombinerClass(Reduce.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // set output key type
    job.setOutputKeyClass(LongWritable.class);
    // set output value type
    job.setOutputValueClass(Text.class);
    // set the HDFS path of the input data
    FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
    // set the HDFS path for the output
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
    // Wait till job completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}