import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CustomerRating{
	// Mapper
	public static class RateMap extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text result = new Text();
		private Text keyValue = new Text(); // type of output key
		private Set<String> businessSet = new HashSet<String>();
		public String haha="";
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			try{
				//Path[] businessFiles = DistributedCache
				//		.getLocalCacheFiles(context.getConfiguration());
				URI[] businessFiles =context.getCacheFiles();
				FileSystem fs=FileSystem.get(context.getConfiguration());
				if (businessFiles != null&&businessFiles.length>0) {
					for (URI businessItem : businessFiles) {
						Path path=new Path(businessItem.toString());
						FSDataInputStream fsin=fs.open(path);
						DataInputStream in=new DataInputStream(fsin);
						try {
							BufferedReader bufferedReader = new BufferedReader(
									new InputStreamReader(in));
							String line=null;
							while ((line = bufferedReader.readLine()) != null) {
								String[] lineArray=line.toString().split("::");
								String address=lineArray[1];
								if(address.contains("Palo Alto")){
									businessSet.add(lineArray[0]);
								}
							}
							bufferedReader.close();
						} catch (Exception e) {
							System.err.println("Exception while reading stop words file: "
									+ e.getMessage());
						}
					}
				}

			} catch (Exception ex) {
				System.err.println("Exception in mapper setup: "
						+ ex.getMessage());
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] lineArray = value.toString().split("::");
			if (businessSet.contains(lineArray[2].toString())) {
				keyValue.set(lineArray[1]);
				result.set(lineArray[3]+haha);
				context.write(keyValue,result);
			}
		}
		/*
		private void readFile(DataInputSteam in) {
			try {
				BufferedReader bufferedReader = new BufferedReader(
						new InputSteamReader(in));
				String line=null;
				while ((line = bufferedReader.readLine()) != null) {
					String[] lineArray=line.toString().split("::");
					String address=lineArray[1];
					if(address.contains("Palo Alto")){
						businessSet.add(lineArray[0]);
					}
				}
				bufferedReader.close();
			} catch (Exception e) {
				System.err.println("Exception while reading stop words file: "
						+ e.getMessage());
			}

		}*/
	}

	// Reducer
	public static class RateReduce extends
			Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			double sum=0.0;
			int count=1;
			for (Text score : values) {
				double score_d=Double.parseDouble(score.toString());
				count+=1;
				sum+=score_d;
			}
			double avg_rate=sum/(double)count;
			String resultString=avg_rate+"";
			result.set(resultString);
			context.write(key, result);// create a pair <keyword, number of
										// occurences>
		}
	}

	// Driver
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();	// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: CountYelpReview <in> <out> <additional>");
			System.exit(-1);
		}


		Job job = new Job(conf,"BusinessTen");
		job.setJarByClass(CustomerRating.class);


		job.setNumReduceTasks(1);
		
		job.addCacheFile(new Path(otherArgs[2]).toUri());
		//DistributedCache.addCacheFile(new Path(otherArgs[2]).toUri(),job.getConfiguration());
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// set the HDFS path for the output
		
		job.setMapperClass(RateMap.class);
		job.setReducerClass(RateReduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);

		
		// set output value type
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);

		//Wait till job completion
		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.exit(job.waitForCompletion(true)?0:1);
		}
}