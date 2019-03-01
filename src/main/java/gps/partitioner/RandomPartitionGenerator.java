package gps.partitioner;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class RandomPartitionGenerator {

	public static void main(String[] args) throws ParseException, NumberFormatException,
		IOException {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("nn", "numberOfNodes", true, "");
		options.addOption("nc", "numberOfClusters", true, "");
		options.addOption("od", "outputdir", true, "");
		options.addOption("of", "outputFile", true, "");
		CommandLine commandLine = parser.parse(options, args);
		int numNodes = Integer.parseInt(commandLine.getOptionValue("nn"));
		int numClusters = Integer.parseInt(commandLine.getOptionValue("nc"));
		String outputFileName =
			commandLine.getOptionValue("od") + "/" + commandLine.getOptionValue("of");
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFileName));
		for (int i = 1; i < numNodes + 1; ++i) {
			bufferedWriter.write("" + (i % numClusters) + "\n");
		}
		bufferedWriter.close();
	}
}
