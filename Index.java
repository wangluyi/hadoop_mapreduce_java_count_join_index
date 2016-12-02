package assignment2;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.swing.text.Keymap;
import javax.ws.rs.core.NewCookie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.log4j.BasicConfigurator;

public class Index {

	public static class InvertedIndexMapper extends
			Mapper<Object, Text, Text, Text> {

		Set<String> stopWords;

		@Override
		public void setup(Context context) throws IOException {
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			line = line.replaceAll(",", " ");
			line = line.replaceAll("\\.", " ");
			line = line.replaceAll("-", " ");
			
			StringTokenizer tokenizer = new StringTokenizer(line); // treat consecutive spaces as one
			String movieId = tokenizer.nextToken();
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken().toLowerCase();
				context.write(new Text(word), new Text(movieId));
			}
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) //input<word, movieID>
				throws IOException, InterruptedException {
       
			HashMap<String, Integer> MovieCountPair = new HashMap<String, Integer>();
			// use hashmap to store <movieID, count> for a certain word
			for (Text value : values) {
				String valueToString = value.toString();
	            if(MovieCountPair.containsKey(valueToString)){ // has this movieID
	            	MovieCountPair.put(valueToString, MovieCountPair.get(valueToString) + 1);
	            }
            	else{
            		MovieCountPair.put(valueToString, 1);
            	}
			}
			String MovieCount_str = "";
			for (String MovieIDKey : MovieCountPair.keySet()) {				
				MovieCount_str += MovieIDKey + "," + Integer.toString(MovieCountPair.get(MovieIDKey)) + " ";
		    }
			MovieCount_str = MovieCount_str.substring(0, MovieCount_str.length()-1); // delete last space" "

			context.write(key, new Text(MovieCount_str));
		}
	}

	public static void main(String[] args) throws Exception {
		
		//BasicConfigurator.configure();
		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Index.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		if (args.length > 2){
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		}

		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		

		FileInputFormat.addInputPath(job, new Path(args[0]+"data/plot_summaries.txt"));
		FileSystem fs = FileSystem.get(conf);
		
		// handle (e.g. delete) existing output path
		Path outputDestination = new Path(args[0]+args[1]);
		if (fs.exists(outputDestination)) {
			fs.delete(outputDestination, true);
		}

		// set output path & start job1
		FileOutputFormat.setOutputPath(job, outputDestination);
		int jobCompletionStatus = job.waitForCompletion(true) ? 0 : 1;

		// job.submit();
	}
}
