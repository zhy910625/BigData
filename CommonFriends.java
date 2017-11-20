import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class CommonFriends {
	
	public static HashSet<String> friendSet=new HashSet<String>();

	public static class Map
	extends Mapper<LongWritable, Text, Text, Text>{

		private final static Text friend = new Text();
		private Text id = new Text(); // type of output key

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("\t");
			String user=mydata[0];
			friend.set(user);
			if(mydata.length==2){
			String[] friends=mydata[1].split(",");
			for (int i=0;i<friends.length;i++) {
				Long userLong=Long.parseLong(user);
				Long iLong=Long.parseLong(friends[i]);
				if(userLong<iLong){
					String hashValue=user+","+friends[i];
					friendSet.add(hashValue);
				}
				else{
					String hashValue=friends[i]+","+user;
					friendSet.add(hashValue);
				}
				for(int j=i+1;j<friends.length;j++){
					Long jLong=Long.parseLong(friends[j]);
					if(iLong<jLong){
						String key_value=friends[i]+","+friends[j];
						id.set(key_value);
					}
					else{
						String key_value=friends[j]+","+friends[i];
						id.set(key_value);
					}
					context.write(id, friend);
				}
			}
			}
		}
	}
	
	public static class Reduce
	extends Reducer<Text,Text,Text,Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String friend_list =""; // initialize common friend list
			String key1=key.toString();
			if(friendSet.contains(key1)){
			if(key1.equals("0,4")||key1.equals("20,22939")||key1.equals("1,29826")||key1.equals("6222,19272")||key1.equals("28041,28056")){
				for (Text val : values) {
					friend_list +=","+val;
				}
				friend_list=friend_list.substring(1);
				result.set(friend_list);
				context.write(key,result);
			}
			}
		}
	}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "CommonFriends");
		job.setJarByClass(CommonFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);


		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
