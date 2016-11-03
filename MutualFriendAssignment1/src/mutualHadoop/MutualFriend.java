package mutualHadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriend {

	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		 // type of output key
		private Text word = new Text();
		private Text mapValues = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("\t");
			String result="";
			if(mydata.length == 2){
			int fromFriend = Integer.parseInt(mydata[0]);
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
			    
			    word.set(result);
			    mapValues.set(removeFriend(list,result).toString());
				context.write(word,mapValues);
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
										// occurences>
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriend <in> <out>");
			System.exit(2);
		}
		// create a job with name "mutualfriend"
		Job job = new Job(conf, "mutualfriend");
		job.setJarByClass(MutualFriend.class);
		

        //job.setMapOutputKeyClass(Pair.class);
        //job.setMapOutputValueClass(Set.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		// uncomment the following line to add the Combiner
	
        
		// set output key type
		job.setOutputKeyClass(Text.class);
		
		// set output value type
		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
