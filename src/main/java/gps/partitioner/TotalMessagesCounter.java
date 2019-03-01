package gps.partitioner;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class TotalMessagesCounter {

	public static void main(String[] args) throws ParseException, NumberFormatException,
		IOException {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("nc", "numberofclusters", true, "");
		options.addOption("cf", "clusterfile", true, "");
		options.addOption("if", "inputfile", true, "");
		options.addOption("nn", "numberOfNodes", true, "");
		CommandLine commandLine = parser.parse(options, args);
		int numberOfClusters = Integer.parseInt(commandLine.getOptionValue("nc"));
		int numNodes = Integer.parseInt(commandLine.getOptionValue("nn"));

		System.out.println("numNodes: " + numNodes);
		long startTime = System.currentTimeMillis();
		String inputFile = commandLine.getOptionValue("if");
		Graph graphObj = PartitionerUtils.getGraph(inputFile, numNodes, false /* directed */);
		String clusterFile = commandLine.getOptionValue("cf");
		int[] clusterMap = PartitionerUtils.getClusterMap(clusterFile, numNodes);

		int[][] graph = graphObj.graph;

		int[] numMessagesPerCluster =
			PartitionerUtils.countTotalNumMessagesSent(numberOfClusters, clusterMap, graph);
		int[] clusterSizes = PartitionerUtils.findClusterSizes(clusterFile, numberOfClusters);
		int totalMessages = 0;
		for (int i = 0; i < numMessagesPerCluster.length; ++i) {
			int clusterMessages = numMessagesPerCluster[i];
			System.out.println("cluster " + i + " size: " + clusterSizes[i] + " messages: "
				+ clusterMessages);
			totalMessages += clusterMessages;
		}
		System.out.println("TotalMessages: " + totalMessages);
		long endTime = System.currentTimeMillis();
		System.out.println("TotalTime: " + (endTime - startTime));
	}
}
