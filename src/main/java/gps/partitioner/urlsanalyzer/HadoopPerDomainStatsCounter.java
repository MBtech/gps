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
 * Input : output of HadoopPerVertexIncomingOutgoingEdgesCounter <id, domain, #outdegree, #indegree>
 * Output: <domain, #vertices, #outdegree, #indegree>
 *
 * @author semihsalihoglu
 */
public class HadoopPerDomainStatsCounter {

	public static class Map extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException {
			String line = value.toString();
			String[] split = line.split("\\s+");
			if (split.length >= 2) {
				try {
					output.collect(new Text(split[1]), new IntWritable(-1 * Integer.parseInt(split[2])));
					output.collect(new Text(split[1]), new IntWritable(Integer.parseInt(split[3])));
				} catch (NumberFormatException e) {
					// Do nothing.
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, Text> {
		private int tmpInt;

		public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			int outDegree = 0;
			int inDegree = 0;
			int numVerticesTimesTwo = 0;
			while (values.hasNext()) {
				numVerticesTimesTwo++;
				tmpInt = values.next().get();
				if (tmpInt < 0) {
					outDegree += tmpInt * -1;
					continue;
				} else {
					inDegree += tmpInt;
				}
			}
			if ((numVerticesTimesTwo % 2) == 1) {
				System.err.println("numVerticesTimesTwo should not be odd. Each mapper should be" +
					" outputtting exactly 2 values for each vertex of the domain.");
			}
			output.collect(key, new Text((numVerticesTimesTwo/2) + " " + outDegree + " " + inDegree));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(HadoopPerDomainStatsCounter.class);
		conf.setJobName("hadoopperdomainstatscounter");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		if (args.length > 2) {
			conf.setNumReduceTasks(Integer.parseInt(args[2]));
		}
		JobClient.runJob(conf);
	}
}
