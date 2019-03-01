package gps.partitioner;

import java.io.IOException;
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class HadoopNormalizerAndSparsifier {

	public static final String SPARSIFICATION_PERCENTAGE_OPT_NAME = "sparsificationpercentage";
	public static final String SPARSIFICATION_PERCENTAGE_SHORT_OPT_NAME = "sp";
	public static final String NORMALIZE_OPT_NAME = "normalize";
	public static final String NORMALIZE_SHORT_OPT_NAME = "normalize";

	public static class Map extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text> {
		public float sparsificationPercentage = -1;
		
		@Override
		public void configure(JobConf conf) {
			sparsificationPercentage = conf.getFloat(SPARSIFICATION_PERCENTAGE_OPT_NAME, -1);
			System.out.println("Sparsification Percentage: " + sparsificationPercentage);
		}

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output,
			Reporter reporter) throws IOException {
			if (sparsificationPercentage > 0 && Math.random() > sparsificationPercentage) {
				return;
			}
			String line = value.toString();
			String[] split = line.split("\\s+");
			if (split.length >= 2) {
				try {
					for (int i = 1; i < split.length; ++i) {
						output.collect(new IntWritable(Integer.parseInt(split[0])), new Text(split[i]));
					}
				} catch (NumberFormatException e) {
					// Do nothing.
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, Text> {
		public boolean normalize = true;
		int tmpInt;

		@Override
		public void configure(JobConf conf) {
			normalize = conf.getBoolean(NORMALIZE_OPT_NAME, true);
//			System.out.println("normalize: " + normalize);
		}

		public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			if (normalize) {
				outputNormalized(key, values, output);
			} else {
				outputUnnormalized(key, values, output);
			}
		}

		private void outputUnnormalized(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, Text> output) throws IOException {
			while (values.hasNext()) {
				output.collect(key, new Text("" + values.next()));
			}
		}

		private void outputNormalized(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, Text> output) throws IOException {
			List<Integer> valuesIntList = new ArrayList<Integer>(); 
			int sizeOfOutputString = 0;
			while (values.hasNext()) {
				tmpInt = Integer.parseInt(values.next().toString());
				valuesIntList.add(tmpInt);
//				sizeOfOutputString = (tmpInt / 10) + 2;
				sizeOfOutputString += Integer.toString(tmpInt).length() + 2;
			}
			StringBuffer neighbors = new StringBuffer(sizeOfOutputString);
			for (int valueInt : valuesIntList) {
				neighbors.append(valueInt);
				neighbors.append(" ");
//				neighbors += values.next() + " ";
			}
			output.collect(key, new Text(neighbors.toString().trim()));			
		}
	}

	public static void main(String[] args) throws Exception {
		CommandLine line = parseAndAssertCommandLines(args);

		JobConf conf = new JobConf(HadoopNormalizerAndSparsifier.class);
		conf.setJobName("hadoopnormalizerandsparsifier");

		if (line.hasOption(SPARSIFICATION_PERCENTAGE_OPT_NAME)) {
			conf.setFloat(SPARSIFICATION_PERCENTAGE_OPT_NAME,
				Float.parseFloat(line.getOptionValue(SPARSIFICATION_PERCENTAGE_OPT_NAME)));
		}
		if (line.hasOption(NORMALIZE_OPT_NAME)) {
			conf.setBoolean(NORMALIZE_OPT_NAME,
				Boolean.parseBoolean(line.getOptionValue(NORMALIZE_OPT_NAME)));
		}
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
//		conf.setNumMapTasks(14);
//		conf.setNumReduceTasks(14);

		conf.setMapperClass(Map.class);

		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

	private static CommandLine parseAndAssertCommandLines(String[] args) {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption(SPARSIFICATION_PERCENTAGE_OPT_NAME,
			SPARSIFICATION_PERCENTAGE_SHORT_OPT_NAME, true,
			"if sparsifying, the percentage of edges to keep");
		options.addOption(NORMALIZE_OPT_NAME, NORMALIZE_SHORT_OPT_NAME, true,
			"whether the output should be normalized or not");
		try {
			CommandLine line = parser.parse(options, args);
			return line;
		} catch (ParseException e) {
			System.err.println("Unexpected exception:" + e.getMessage());
			System.exit(-1);
			return null;
		}
	}
}
