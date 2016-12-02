package assignment2;

import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.log4j.BasicConfigurator;

public class Join {

	
	public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String fileTag = "movieTag~";
		
		@Override
		public void setup(Context context) throws IOException {
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
			String line = value.toString();
			String[] columns = line.split("\t");
			String movieID = columns[0].trim();
			String movieName = columns[2].trim();
				
			context.write(new Text(movieID), new Text(fileTag + movieName));
		}
	}


	public static class ActMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String fileTag = "actTag~";

		@Override
		public void setup(Context context) throws IOException {
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] columns = line.split("\t");
			String movieID = columns[0];
			String actName = columns[8];
			context.write(new Text(movieID), new Text(fileTag + actName));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
		}
	}


	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		
		private String movieName, actName;
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			actName = "";
			for (Text value : values) {
				String currValue = value.toString();
	            String[] splitVals = currValue.split("~");
	            if(splitVals.length < 2){
	            	continue;
	            }
	            if(splitVals[0].equals("movieTag")){
	                movieName = splitVals[1] != null ? splitVals[1].trim() : "movieName";
	            } else if (splitVals[0].equals("actTag")){
	                actName += splitVals[1] != null ? splitVals[1].trim() : "actName";
	                actName += ",";
	            }
	        }
			if (actName.length()==0) {
				actName = "no_actor_name,";
			}
			if(actName.substring(actName.length() - 1).equals(",")){
				actName = actName.substring(0, actName.length() - 1);
			}// delete the last "," 
			
			context.write(key, new Text(movieName + "," + actName));
		}
	}


	public static void main(String[] args) throws Exception {
		//BasicConfigurator.configure();
		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "join_job");
		job.setJarByClass(Join.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
		if (args.length > 2){
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		}
	             
	    MultipleInputs.addInputPath(job, new Path(args[0]+"data/movie.metadata.tsv"), 
	    		TextInputFormat.class, MovieMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[0]+"data/character.metadata.tsv"), 
	    		TextInputFormat.class, ActMapper.class);
	    
	    FileSystem fs = FileSystem.get(conf);
		// handle (e.g. delete) existing output path
		Path outputDestination = new Path(args[0] + args[1]);
		if (fs.exists(outputDestination)) {
			fs.delete(outputDestination, true);
		}
	    FileOutputFormat.setOutputPath( job, outputDestination );
		int jobCompletionStatus = job.waitForCompletion(true) ? 0 : 1;
		// job.submit();
		
	}
}
