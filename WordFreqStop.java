package assignment2;

import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.log4j.BasicConfigurator;

public class WordFreqStop {

	public static class CountMapper extends Mapper<Object, Text, Text, IntWritable> {
		Set<String> stopWords = new HashSet<String>();;
		private Text wordText = new Text();  // type of output key

		@SuppressWarnings("deprecation")
		@Override
		public void setup(Context context) throws IOException {

			Path[] uris = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			// It works on local linux if using the fsys and inputstreamreader to open stop.txt
			// but not working on virtual machines
//			FileSystem fsys = FileSystem.get(context.getConfiguration());
//			BufferedReader br = new BufferedReader(new InputStreamReader(fsys.open(uris[0])));
			BufferedReader br = new BufferedReader(new FileReader(uris[0].toString()));
			
			String line;
			while ((line = br.readLine())!= null) {
				stopWords.add(line);
			}
			br.close();
		}
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
		
			// *********pay attention to the replaceAll and replace
			String line = value.toString();
			line = line.replaceAll(",", " ");
			line = line.replaceAll("\\.", " ");
			line = line.replaceAll("-", " ");

			StringTokenizer tokenizer = new StringTokenizer(line);
			int movieId = Integer.parseInt(tokenizer.nextToken());
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken().toLowerCase();
				wordText.set(word);
				if (   stopWords.contains(wordText.toString()) != true   ){
					context.write(wordText, new IntWritable(1));
				}	
			}
		}
	}

	public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
		context.write(key, new IntWritable(sum)); // create a pair <keyword, number>
		}
	}

	public static class TopKMapper extends Mapper<Object, Text, NullWritable, Text> {
		Set<String> stopWords;
		private TreeMap<Integer, Text> countTreeMap = new TreeMap<Integer, Text>();
		
		@Override
		public void setup(Context context) throws IOException {
		}
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] WordCount = line.split("\t");			
			countTreeMap.put(Integer.parseInt(WordCount[1]), new Text(WordCount[0]));
			int TopK = 10;
			if (countTreeMap.size() > TopK) {
				countTreeMap.remove(countTreeMap.firstKey());
		    }
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
		    for (Integer t : countTreeMap.keySet()) {
		        StringBuilder sb = new StringBuilder();
		        sb.append(t.toString() + "\t" + countTreeMap.get(t).toString());
		        context.write(NullWritable.get(), new Text(sb.toString()));
		    }
		}
	}

	public static class TopKReducer extends Reducer<NullWritable, Text, Text, IntWritable> {
		private TreeMap<Integer, Text> countTreeMap = new TreeMap<Integer, Text>();		
		public void reduce(NullWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			for (Text value : values) {
		        String line = value.toString();
		        String[] count_word = line.split("\t"); 
		        countTreeMap.put(Integer.parseInt(count_word[0]), new Text(count_word[1]));
		        
		        int TopK = 10;
		        if (countTreeMap.size() > TopK) {
		        	countTreeMap.remove(countTreeMap.firstKey());
		        }
			}
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
		    for (Integer t : countTreeMap.descendingKeySet()) {
		        context.write(countTreeMap.get(t), new IntWritable(t));
		    }
		}
	}		
	

	public static void main(String[] args) throws Exception {
		//BasicConfigurator.configure();
		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "WordFreqStop job1");
		Path pathURI = new Path(args[0]+"stopwords/stop.txt");
		DistributedCache.addCacheFile(pathURI.toUri(), job.getConfiguration());
		job.setJarByClass(WordFreqStop.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);
		if (args.length > 2){
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		}

		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);		

		FileInputFormat.addInputPath(job, new Path(args[0]+"data/plot_summaries.txt"));
		FileSystem fs = FileSystem.get(conf);
		// handle (e.g. delete) existing output path
		Path outputDestination = new Path(args[0]+"first");
		if (fs.exists(outputDestination)) {
			fs.delete(outputDestination, true);
		}

		// set output path & start job1
		FileOutputFormat.setOutputPath(job, outputDestination);
		int jobCompletionStatus = job.waitForCompletion(true) ? 0 : 1;
		// job.submit();
		
		// Create job2
		Job job2 = Job.getInstance(conf, "WordFreqStop job2");
        job2.setJarByClass(WordFreqStop.class);

        job2.setMapperClass(TopKMapper.class);
        job2.setReducerClass(TopKReducer.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, outputDestination);
        Path outputDestination2 = new Path(args[0]+args[1]);
		if (fs.exists(outputDestination2)) {
			fs.delete(outputDestination2, true);
		}
        TextOutputFormat.setOutputPath(job2, outputDestination2);
		
		int job2CompletionStatus = job2.waitForCompletion(true) ? 0 : 1; // this should not be deleted
		
		if (fs.exists(outputDestination)) {
			fs.delete(outputDestination, true);
		}
	}
}
