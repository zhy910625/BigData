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



public class BusinessTen {
	
	public static class Pair implements Comparable<Pair>{
		public String information;
		public double rate;
		
		public Pair(){
			information="";
			rate=0.0;
		}

		@Override
		public int compareTo(Pair newPair) {
			int result=0;
			double diff=newPair.rate-this.rate;
			if(diff>0.0){
				result=1;
			}
			else if(diff<0.0){
				result=-1;
			}
			return result;
		}	
	}
	
	public static PriorityQueue<Pair> resultQueue=new PriorityQueue<Pair>();
	
	public static class BusinessMap extends Mapper<LongWritable, Text, Text, Text>{
		public Text keyWord=new Text();
		public Text valueWord=new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] lineArray=value.toString().split("::");
			if(lineArray.length==3){
				keyWord.set(lineArray[0]);
				valueWord.set(lineArray[1]+","+lineArray[2]);
			}
			else if(lineArray.length==4){
				keyWord.set(lineArray[2]);
				valueWord.set(lineArray[3]);
			}
			context.write(keyWord, valueWord);
		}
	}

	public static class BusinessReduce extends Reducer<Text,Text,Text,Text> {
		Text result = new Text();
		Text resultKey=new Text();
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			int count=0;
			double sum = 0.0;
			resultKey.set(key.toString());
			for(Text val : values){
				if(val.toString().contains(",")){
					String[] valArray=val.toString().split(",");
					String newKey=key.toString()+","+valArray[0]+","+valArray[1];
					resultKey.set(newKey);
				}
				else{
					count+=1;
					sum+=Double.parseDouble(val.toString());
				}
			}
			double avg = ((double)sum/(double)count);
			result.set(avg+"");
			context.write(resultKey,result);
		}
	}

	public static class tenMap extends Mapper<LongWritable, Text, Text, Text>{
		public Text keyWord=new Text();
		public Text valueWord=new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] lineArray=value.toString().split("\t");
			if(lineArray.length==2){
				keyWord.set(lineArray[0]);
				valueWord.set(lineArray[1]);
			}
			context.write(keyWord, valueWord);
		}
	}
	
	public static class tenReduce extends Reducer<Text,Text,Text,Text> {
		Text result = new Text();
		Text resultKey=new Text();
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			Pair newPair=new Pair();
			newPair.information=key.toString();
			for(Text val:values){
				newPair.rate=Double.parseDouble(val.toString());
			}
			resultQueue.add(newPair);
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			for(int i=0;i<10;i++){
				Pair head=resultQueue.poll();
				String[] informationArray=head.information.toString().split(",");
				resultKey.set(informationArray[0]);
				result.set(informationArray[1]+" "+informationArray[2]+" "+head.rate);
				context.write(resultKey,result);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
		/*if (otherArgs.length != 2) {
			System.err.println("Usage: CountYelpReview <in> <out>");
			System.exit(2);
		}*/


		Job job = new Job(conf, "BusinessTen");
		job.setJarByClass(BusinessTen.class);


		job.setNumReduceTasks(1);
		
		job.setMapperClass(BusinessMap.class);
		job.setReducerClass(BusinessReduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);


		// set output value type
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);


		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		//Wait till job completion
		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		/*********Second Job***************/
		Configuration conf2 = new Configuration();

		Job job2 = Job.getInstance(conf2, "BusinessTen");
		job2.setJarByClass(BusinessTen.class);

		Path secondInput = new Path(otherArgs[2]+"/part-r-00000");
		Path finalOutput = new Path(otherArgs[3]);

		FileInputFormat.addInputPath(job2, secondInput);
		//MultipleInputs.addInputPath(job2, secondInput, TextInputFormat.class, MyJob.MapClass3.class);
		//job2.setPartitionerClass(MyJob.MyPartition.class);

		FileOutputFormat.setOutputPath(job2, finalOutput);
		job2.setMapperClass(tenMap.class);
		job2.setReducerClass(tenReduce.class);
		//job2.setOutputFormatClass(Text.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.waitForCompletion(true);
		System.exit(job2.waitForCompletion(true)?0:1);
		}
}