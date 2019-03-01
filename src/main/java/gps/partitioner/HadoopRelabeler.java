package gps.partitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
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
 * Given a) a graph file in Jure's format (i.e unnormalized, each line contains a single edge in the
 * form of 0 4, 1 333, 2 122) b) a newIds file in the format of (-1 0 14) (meaning -1 to signal   
 * that this is not the unnormalized graph file, 0 previous id and 14 is the new id), and c) the
 * column number being relabeled (1 or 2) outputs a graph file with the given column relabeled with
 * the new ids.
 *
 * @author semihsalihoglu
 */
public class HadoopRelabeler {

	public static final String COLUMN_NUMBER_OPT_NAME = "columnnumber";
	public static final String COLUMN_NUMBER_SHORT_OPT_NAME = "cn";

	public static class Map extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, IntWritable> {

		String[] split;
		String remainingLine;
		String line;
		public boolean isFirstColumn = false;

		@Override
		public void configure(JobConf conf) {
			isFirstColumn = conf.getBoolean(COLUMN_NUMBER_OPT_NAME, false);
		}
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output,
			Reporter reporter) throws IOException {
			line = value.toString();
			split = line.split("\\s+");
			if (split.length < 2) {
				return;
			}
//			System.out.println("mapper isFirstColumn: " + isFirstColumn);
			int firstValueOnLine = Integer.parseInt(split[0]);
			int secondValueOnLine = Integer.parseInt(split[1]);

			int intermediateKeyValue;
			int intermediateValueValue;


			if (firstValueOnLine == -1) {
				intermediateKeyValue = secondValueOnLine;
				intermediateValueValue = -1 * Integer.parseInt(split[2]);
			} else {
				// If the first column is being relabeled
				if (isFirstColumn) {
					intermediateKeyValue = firstValueOnLine;
					intermediateValueValue = secondValueOnLine;
				} else {
					intermediateKeyValue = secondValueOnLine;
					intermediateValueValue = firstValueOnLine;
				}
			}
//			System.out.println("Mapping output: key: " + intermediateKeyValue
//				+ " value: " + intermediateValueValue);
			// If this is a newIds file line send as key the second column
			// and as value the negative of the new id
			// System.out.println("Mapping key: " + key.get() + " value: " + value.toString()
			// + " to intermediate_key: " + intermediateKey.get() +
			// " intermediate_value: " + intermediateValue.toString() +
			// " assigned partition no: " + Integer.parseInt(split[0]));
			output.collect(new IntWritable(intermediateKeyValue),
				new IntWritable(intermediateValueValue));
		}
	}

	public static class Reduce extends MapReduceBase implements
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		
		public boolean isFirstColumn = false;

		@Override
		public void configure(JobConf conf) {
			isFirstColumn = conf.getBoolean(COLUMN_NUMBER_OPT_NAME, false);
		}

		public void reduce(IntWritable key, Iterator<IntWritable> values,
			OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
//			System.out.print("reducer isFirstColumn: " + isFirstColumn + "\t");
			Integer newValue = null;
//			System.out.print("Reducing key: " + key.get() + " \t");
			List<Integer> valuesList = new ArrayList<Integer>();
			while (values.hasNext()) {
				int value = values.next().get();
//				System.out.print("value: " + value + " \t");
				if (value < 0) {
					newValue = -1 * value;
				} else {
					valuesList.add(value);
				}
			}
//			System.out.println();

			if (newValue == null) {
				System.err.println("New Value was not found!");
				reporter.incrCounter("reducer", "no-new-value-for-id", 1);
				reporter.incrCounter("reducer", "no-new-value-vertex-id", key.get());
				newValue = key.get();
			} else {
//				System.out.println("New Value: " + newValue);
			}

			for (int value : valuesList) {
				String outputLine = null;
				if (isFirstColumn) {
//					System.out.println("isFirstColumn: reducer output: " + newValue + " " + value);
					output.collect(new IntWritable(newValue), new IntWritable(value));
				} else {
//					System.out.println("isSecondColumn: reducer output: " + value + " " + newValue);
					output.collect(new IntWritable(value), new IntWritable(newValue));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		CommandLine line = parseAndAssertCommandLines(args);
		boolean isFirstColumn = false;
		if (line.hasOption(COLUMN_NUMBER_OPT_NAME)) {
			isFirstColumn = (Integer.parseInt(line.getOptionValue(COLUMN_NUMBER_OPT_NAME)) == 1);
		}
		System.out.println("master isFirstColumn: " + isFirstColumn);
		JobConf conf = new JobConf(HadoopRelabeler.class);
		conf.setBoolean(COLUMN_NUMBER_OPT_NAME, isFirstColumn);
		conf.setJobName("hadooprelabeler");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
//		conf.setNumMapTasks(1);
//		conf.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(conf, new Path(args[0]), new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		JobClient.runJob(conf);
	}

	private static CommandLine parseAndAssertCommandLines(String[] args) {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption(COLUMN_NUMBER_OPT_NAME, COLUMN_NUMBER_SHORT_OPT_NAME, true,
			"column number that is being relabeled." +
			"in general this hadoop job should be run twice, first with column number = 1, then with column number=2");
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
