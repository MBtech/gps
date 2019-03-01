package gps.partitioner;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class HadoopDirectedToUndirectedNormalizedConverter {

	public static class Map extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException {
			String line = value.toString();
			String[] split = line.split("\\s+");
			Integer.parseInt(split[0]);
			if (split.length >= 2) {
//				try {
					for (int i = 1; i < split.length; ++i) {
						Integer.parseInt(split[i]);
						output.collect(new Text(split[0]), new IntWritable(Integer.parseInt(split[i])));
						output.collect(new Text(split[i]), new IntWritable(Integer.parseInt(split[0])));
					}
					//					output.collect(new Text(split[0] + " " + split[1]), new Text(""));
//					output.collect(new Text(split[1] + " " + split[0]), new Text(""));
//				} catch (NumberFormatException e) {
//					// Do nothing.
//				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, Text> {
		private int tmpInt;
		
		public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
//			try {
				int vertexId = Integer.parseInt(key.toString());
//			} catch (NumberFormatException e) {
//				System.err.println("key is not a valid number: " + key.toString() + " returning...");
//				return;
//			}
			Set<Integer> intValues = new HashSet<Integer>();
			int intSizeOfOutputString = 0;
//			long longSizeOfOutputString = 0;
			while (values.hasNext()) {
				tmpInt = values.next().get();
				if (tmpInt < 0) {
					System.err.println("neighborId has a negative id: " + tmpInt);
					throw new RuntimeException("neighborId has a negative id: " + tmpInt);
				}
//				if (intValues.contains(tmpInt)) {
//					continue;
//				} else {
					intValues.add(tmpInt);
					intSizeOfOutputString += Integer.toString(tmpInt).length() + 2;
//					output.collect(key, new Text("" + tmpInt));
//					longSizeOfOutputString += ((tmpInt / 10) + 2);
//				}
			}
			StringBuffer neighbors = new StringBuffer(intSizeOfOutputString);
			for (int valueInt : intValues) {
				neighbors.append("" + valueInt + " ");
//				neighbors.append(" ");
//				neighbors += values.next() + " ";
			}
			output.collect(key, new Text(neighbors.toString().trim()));	
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Starting a new version 4...");
		JobConf conf = new JobConf(HadoopDirectedToUndirectedNormalizedConverter.class);
		conf.setJobName("hadoopdirectedtoundirectedconverter");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
//		conf.setNumMapTasks(300);
//		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setNumReduceTasks(24);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}	
}