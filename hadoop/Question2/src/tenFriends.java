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
import java.util.PriorityQueue;

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


public class tenFriends {
	
	public static HashSet<String> friendSet=new HashSet<String>();
	/**
	 * @param args
	 */
	public static class PairClass implements Comparable<PairClass> {
		public String key;
		public int count;
		
		public PairClass(){
			key="";
			count=0;
		}
		
		public int compareTo(PairClass newPair){
			return newPair.count-this.count;
		}
	}
	public static PriorityQueue<PairClass> resultQueue=new PriorityQueue<PairClass>();

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
	extends Reducer<Text,Text,Text,IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String friend_list =""; // initialize common friend list
			String key1=key.toString();
			int sum=0;
			if(friendSet.contains(key1)){
				for (Text val : values) {
					sum+=1;
				}
				PairClass newPair=new PairClass();
				newPair.key=key1;
				newPair.count=sum;
				resultQueue.add(newPair);
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			for(int i=0;i<10;i++){
				PairClass head=resultQueue.poll();
				context.write(new Text(head.key),new IntWritable(head.count));
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
		job.setJarByClass(tenFriends.class);
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
