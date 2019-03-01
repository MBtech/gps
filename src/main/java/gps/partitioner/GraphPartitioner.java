package gps.partitioner;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class GraphPartitioner {

	public static void main(String[] args) throws ParseException, NumberFormatException,
		IOException {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("np", "numberofpartitions", true, "number of partitions");
		options.addOption("nn", "numberofnodes", true, "number of nodes in the graph");
		options.addOption("if", "inputfile", true, "location of the machine configuration file");
		options.addOption("od", "outputdir", true, "location of the machine configuration file");
		options.addOption("ofp", "outputfileprefix", true,
			"location of the machine configuration file");
		CommandLine commandLine = parser.parse(options, args);
		int numPartitions = Integer.parseInt(commandLine.getOptionValue("np"));
		String fullOutputFilePrefix =
			commandLine.getOptionValue("od") + "/" + commandLine.getOptionValue("ofp");
		System.out.println("fullOutputFilePrefix: " + fullOutputFilePrefix);
		PartitionerUtils.writeGraphInPartitions(
			PartitionerUtils.getGraph(commandLine.getOptionValue("if"),
				Integer.parseInt(commandLine.getOptionValue("nn")), false /* directed */).graph,
			fullOutputFilePrefix, numPartitions);
	}
}
