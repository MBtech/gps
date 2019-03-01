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

public class HadoopPartitioner {

	// Whether the file is a weighted edge representation, in which case
	// the last number on each line should be ignored.
	public static final String NUM_PARTITIONS_OPT_NAME = "numpartitions";
	public static final String NUM_PARTITIONS_SHORT_OPT_NAME = "np";
	public static final String NUM_MAPPERS_OPT_NAME = "nummappers";
	public static final String NUM_MAPPERS_SHORT_OPT_NAME = "nm";

	public static int numPartitions = 8;

	public static class Map extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text> {

		String[] split = null;
		String line;

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output,
			Reporter reporter) throws IOException {
			line = value.toString();
			// split = line.split("\\s+");
			int tabIndex = line.indexOf("\t");
			if (tabIndex < 0) {
				tabIndex = line.indexOf(" ");
			}
			if (tabIndex < 0) {
				return;
			}
			IntWritable intermediateKey =
				new IntWritable(Integer.parseInt(line.substring(0, tabIndex)));
			// String newLine = "";
			// for (int i = 1; i < split.length; ++i) {
			// newLine += split[i] + " ";
			// }
			Text intermediateValue = new Text(line.substring(tabIndex + 1));
			// System.out.println("Mapping key: " + key.get() + " value: " + value.toString()
			// + " to intermediate_key: " + intermediateKey.get() +
			// " intermediate_value: " + intermediateValue.toString() +
			// " assigned partition no: " + Integer.parseInt(split[0]));
			output.collect(intermediateKey, intermediateValue);
		}
	}

	public static class Reduce extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				String line = values.next().toString();
				// System.out.println("value: " + line);
				// String[] split = line.split("\\s+");
				// String newLine = "";
				// for (int i = 1; i < split.length; ++i) {
				// newLine += split[i] + " ";
				// }
				IntWritable outputKey = key;
				Text outputValue = new Text(line);
				// System.out.println("Reducing key: " + key.get() + " value: " + line +
				// " outputKey: " + outputKey.get() + " outputValue: " + outputValue.toString());
				output.collect(outputKey, outputValue);
			}
		}
	}

	public static class ModPartitioner implements Partitioner<IntWritable, Text> {

		@Override
		public void configure(JobConf arg0) {
			// Nothing to do.
		}

		@Override
		public int getPartition(IntWritable key, Text value, int numReducers) {
			// System.out.println("Partitioning key: " + key.get() + " value: "
			// + value.toString() + " numReducers: " + numReducers);
			return key.get() % numReducers;
		}

	}

	public static void main(String[] args) throws Exception {
		CommandLine line = parseAndAssertCommandLines(args);
		if (line.hasOption(NUM_PARTITIONS_OPT_NAME)) {
			numPartitions = Integer.parseInt(line.getOptionValue(NUM_PARTITIONS_OPT_NAME));
		}
		JobConf conf = new JobConf(HadoopPartitioner.class);
		conf.setJobName("hadooppartitioner");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
//		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setPartitionerClass(ModPartitioner.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setNumReduceTasks(numPartitions);
		if (line.hasOption(NUM_MAPPERS_OPT_NAME)) {
			System.out.println("Setting the number of mappers to: "
				+ line.getOptionValue(NUM_MAPPERS_OPT_NAME));
			conf.setNumMapTasks(Integer.parseInt(line.getOptionValue(NUM_MAPPERS_OPT_NAME)));
		}
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

	private static CommandLine parseAndAssertCommandLines(String[] args) {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption(NUM_PARTITIONS_OPT_NAME, NUM_PARTITIONS_SHORT_OPT_NAME, true,
			"number of partitions");
		options.addOption(NUM_MAPPERS_OPT_NAME, NUM_MAPPERS_SHORT_OPT_NAME, true,
			"number of mappers");
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
