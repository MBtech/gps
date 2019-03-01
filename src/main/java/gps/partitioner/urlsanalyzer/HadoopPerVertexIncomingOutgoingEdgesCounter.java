package gps.partitioner.urlsanalyzer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Input 1: Adjacency list file for a graph Output: <id, # outDegree, # inDegree>
 * Input 2: output of UrlsWithIdsGenerator: <-2, id, url>
 * 
 * Output: <id, <opt-url>, outDegree, inDegree>
 * @author semihsalihoglu
 */
public class HadoopPerVertexIncomingOutgoingEdgesCounter {

	public static class Map extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
			String line = value.toString();
			String[] split = line.split("\\s+");
			int firstValueOnLine = Integer.parseInt(split[0]);
			if (firstValueOnLine == -2) {
				output.collect(new Text(split[1]), new Text(split[2]));
			} else if (split.length >= 2) {
				try {
					output.collect(new Text(split[0]), new Text("" + (-1 * (split.length - 1))));
					//output.collect(new Text(split[0]), new IntWritable(-1 * (split.length - 1)));
					for (int i = 1; i < split.length; ++i) {
						output.collect(new Text(split[i]), new Text("1"));
					}
				} catch (NumberFormatException e) {
					// Do nothing.
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
		private String tmpStr;
		private int tmpInt;
		public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			int vertexId = -1;
			try {
				vertexId = Integer.parseInt(key.toString());
			} catch (NumberFormatException e) {
				System.err
					.println("key is not a valid number: " + key.toString() + " returning...");
				return;
			}
			int outDegree = -1;
			int inDegree = 0;
			String domainName = null;
			while (values.hasNext()) {
				tmpStr = values.next().toString();
				if (tmpStr.contains("http")) {
					domainName = tmpStr.replaceFirst("http://", "");
					domainName = domainName.split("/")[0];
				} else {
					tmpInt = Integer.parseInt(tmpStr);
					if (tmpInt < 0) {
						outDegree = tmpInt * -1;
						continue;
					} else {
					if (tmpInt != 1) {
						System.err.println("Positive values should all be 1. " +
							"This should never happen. value: " + tmpStr);
					} else {
						inDegree++;
					}
				 }
			  }
			}
			if (domainName != null) {
				output.collect(key, new Text(domainName + " " + outDegree + " " + inDegree));
			} else {
				output.collect(key, new Text(outDegree + " " + inDegree));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(HadoopPerVertexIncomingOutgoingEdgesCounter.class);
		conf.setJobName("hadooppervertexincomingoutgoingedgescounter");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		int numInputPaths = Integer.parseInt(args[0]);
		int argIndex = 2;
		if (numInputPaths == 2) {
			FileInputFormat.setInputPaths(conf, new Path(args[1]), new Path(args[2]));
			argIndex = 3;
		} else {
			FileInputFormat.setInputPaths(conf, new Path(args[1]));			
		}

		FileOutputFormat.setOutputPath(conf, new Path(args[argIndex++]));
		if (args.length > argIndex) {
			conf.setNumReduceTasks(Integer.parseInt(args[argIndex++]));
		}
		JobClient.runJob(conf);
	}
}
