package gps.partitioner;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class SnapToGraclusConverter {
	public static void main(String[] args) throws ParseException, IOException {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("nn", "numberofnodes", true, "");
		options.addOption("if", "inputfile", true, "");
		options.addOption("od", "outputDirectory", true, "");
		options.addOption("of", "outputFile", true, "");
		CommandLine commandLine = parser.parse(options, args);
		int numNodes = -1;
		if (commandLine.hasOption("nn")) {
			numNodes = Integer.parseInt(commandLine.getOptionValue("nn"));
		} else {
			numNodes = PartitionerUtils.countNumNodes(commandLine.getOptionValue("if"));
		}
		Graph graphObj =
			PartitionerUtils
				.getGraph(commandLine.getOptionValue("if"), numNodes, true /* undirected */);
		PartitionerUtils.writeGraphInGraclusFormat(PartitionerUtils.getOutputFileName(commandLine)
			+ "-graclus-format", graphObj);
	}
}
