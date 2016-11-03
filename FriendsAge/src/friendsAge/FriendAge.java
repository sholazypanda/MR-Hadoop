package friendsAge;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import friendsAge.FriendAge.NetworkDataMapper.UserDataMapper;

public class FriendAge {

  // Mapper Class to get user and friends information into HDFS
  public static class NetworkDataMapper
      extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String row = value.toString();
      // key to identify table in the reducer
      String joiningRelation = "R";
      StringTokenizer str = new StringTokenizer(row, "\t");
      String user = str.nextToken();
      String friendsList = new String();
      while (str.hasMoreTokens()) {
        friendsList = str.nextToken();
      }

      context.write(new Text(user),
          new Text(joiningRelation.concat("#").concat(friendsList.toString()
              .replace("[", "").replace("]", "").replace(", ", ","))));

    }

    // Mapper to get user, age, address info into HDFS
    public static class UserDataMapper
        extends Mapper<LongWritable, Text, Text, Text> {
      public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {
        String row = value.toString();
        String joiningRelation = "S";
        String[] friendsInfo = row.split(",");

        if (friendsInfo.length >= 9) {
          String user = friendsInfo[0];
          String dateInString = friendsInfo[9];

          Date date = new Date();
          // convert string to date
          int month = date.getMonth() + 1;
          int year = date.getYear() + 1900;
          int age = year - Integer.parseInt(dateInString.split("/")[2]);

          if (Integer.parseInt(dateInString.split("/")[0]) > month) {
            age--;
          } else if (Integer.parseInt(dateInString.split("/")[0]) == month) {
            int nowDay = date.getDate();

            if (Integer.parseInt(dateInString.split("/")[1]) > nowDay) {
              age--;
            }
          }
          context.write(new Text(user),
              new Text(joiningRelation + "#" + friendsInfo[1] + "," + (age + "") + ","
                  + friendsInfo[3] + "," + friendsInfo[4] + ","
                  + friendsInfo[5] + "," + friendsInfo[6]));
        }

      }
    }
  }

  public static class ReducerJoin extends Reducer<Text, Text, Text, Text> {
    HashMap<String, ArrayList<String>> friendsDataMap = new HashMap<>();

    public void setup(Context context) throws IOException {
      Configuration config = context.getConfiguration();
      String hdfsfilepath = config.get("hdfsfilepath");

      Path pt = new Path("hdfs://cshadoop1" + hdfsfilepath);// Location of
                                                            // file in
                                                            // HDFS
      FileSystem fs = FileSystem.get(config);
      BufferedReader br = new BufferedReader(
          new InputStreamReader(fs.open(pt)));
      String line;
      line = br.readLine();
      while (line != null) {
        StringTokenizer str = new StringTokenizer(line, ",");
        String friend = str.nextToken();
        ArrayList<String> friendDetails = new ArrayList<>();
        while (str.hasMoreTokens()) {
          friendDetails.add(str.nextToken());
        }
        friendsDataMap.put(friend, friendDetails);
        line = br.readLine();
      }
    }

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      // populate the tables
      ArrayList<String> M1 = new ArrayList<>();
      ArrayList<String> M2 = new ArrayList<>();

      for (Text val : values) {
        // if length is 2 splitting on 2
        if (val.toString().split("#").length == 2) {
          String joiningRelation = val.toString().split("#")[0];

          if (joiningRelation.equals("R")) {
            if (val.toString().split("#")[1].split(",").length > 0) {
              M1.add(val.toString().split("#")[1]);
            }
          }

          if (joiningRelation.equals("S")) {
            if (val.toString().split("#")[1].split(",").length > 0) {
              M2.add(val.toString().split("#")[1]);
            }
          }

          // context.write(new Text(R.toString()), new Text(S.toString()));
        }
      }

      if (!M1.isEmpty() && !M2.isEmpty()) {
        // interate through R. Get friends age. Keep track of friends' age
        String[] friends = M1.get(0).split(",");
        String dob = new String();
        Integer maxAge = 0, friendAge = 0;
        ArrayList<String> friendDetails = new ArrayList<>();
        for (int i = 0; i < friends.length; i++) {
          if (friendsDataMap.containsKey(friends[i])) {
            friendDetails = friendsDataMap.get(friends[i]);
            for (int j = 0; j < friendDetails.size(); j++) {
              if (j == 8) {
                // calculate the age of the current friend
                String dateInString = friendDetails.get(j);
                ;

                Date date = new Date();
                // convert string to date
                int month = date.getMonth() + 1;
                int year = date.getYear() + 1900;
                int age = year - Integer.parseInt(dateInString.split("/")[2]);

                if (Integer.parseInt(dateInString.split("/")[0]) > month) {
                  age--;
                } else
                  if (Integer.parseInt(dateInString.split("/")[0]) == month) {
                  int nowDay = date.getDate();

                  if (Integer.parseInt(dateInString.split("/")[1]) > nowDay) {
                    age--;
                  }
                }

                if (age > maxAge) {
                  maxAge = age;
                }
              }
            }
          }
        }
        context.write(key,
            new Text(maxAge.toString() + "," + M2.get(0).toString()));
      }
    }
  }

  // job 2 mapper
  public static class MaxAgeMapper
      extends Mapper<LongWritable, Text, CustomKey, Text> {

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String[] maxAgeWriter = value.toString().split("\t");

      if (maxAgeWriter.length == 2) {
        String line[] = maxAgeWriter[1].split(",");

        context.write(new CustomKey(Integer.parseInt(maxAgeWriter[0]),
            Integer.parseInt(line[0])), new Text(maxAgeWriter[1].toString()));
      }
    }

  }

  // reducer 2
  public static class MaxAgeReducer
      extends Reducer<CustomKey, Text, Text, Text> {
    Integer i = 0;

    public void reduce(CustomKey key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
      for(Text textVal:values)
      {
        if(i<10)
        {
          context.write(new Text(textVal.toString().split(",")[1]), new Text(textVal));
          i++;
        }
      }
    }
  }

  // Driver program
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    // get all args
    if (otherArgs.length != 5) {
      System.err.println(
          "Usage: <socdata> <userdata> <userdata> <outputIntermediate> <output>");
      System.exit(2);
    }

    conf.set("hdfsfilepath", otherArgs[2]);

    Job job1 = new Job(conf, "reducejoin");
    job1.setJarByClass(FriendAge.class);
    job1.setReducerClass(ReducerJoin.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job1, new Path(otherArgs[0]),
        TextInputFormat.class, NetworkDataMapper.class);
    MultipleInputs.addInputPath(job1, new Path(otherArgs[1]),
        TextInputFormat.class, UserDataMapper.class);

    job1.setOutputKeyClass(LongWritable.class);
    job1.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3]));

    int status = job1.waitForCompletion(true) ? 0 : 1;

    Job job2 = new Job(new Configuration(), "output");
    job2.setJarByClass(FriendAge.class);
    FileInputFormat.addInputPath(job2, new Path(otherArgs[3]));
    job2.setMapOutputKeyClass(CustomKey.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setPartitionerClass(CustomPartitioner.class);
    job2.setMapperClass(MaxAgeMapper.class);
    job2.setSortComparatorClass(CustomComparator.class);
    job2.setGroupingComparatorClass(CustomGroupingComparator.class);
    job2.setReducerClass(MaxAgeReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));
    status = job2.waitForCompletion(true) ? 0 : 1;

    System.exit(status);
  }
}
