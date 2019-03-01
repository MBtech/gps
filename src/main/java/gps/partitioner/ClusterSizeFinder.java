package gps.partitioner;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class ClusterSizeFinder {

	public static void main(String[] args) throws ParseException, IOException {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("nc", "numberofclusters", true, "");
		options.addOption("cf", "clusterfile", true, "");
		options.addOption("nn", "numberofnodes", true, "");
		options.addOption("if", "inputfile", true, "");
		options.addOption("od", "outputDirectory", true, "");
		options.addOption("of", "outputFile", true, "");
		CommandLine commandLine = parser.parse(options, args);
		//		int numClusters = Integer.parseInt(commandLine.getOptionValue("nc"));
//		int numNodes = Integer.parseInt(commandLine.getOptionValue("nn"));
//		Graph graphObj =
//			PartitionerUtils
//				.getGraph(commandLine.getOptionValue("if"), numNodes, false /* directed */);
//		int[][] graph = graphObj.graph;
//		ClusterSummary clusterSummary =
//			PartitionerUtils.getClusterSummary(commandLine.getOptionValue("cf"), graphObj,
//				numClusters);
//		PartitionerUtils.writeClusterSummary(PartitionerUtils.getOutputFileName(commandLine),
//			numClusters, graph, clusterSummary);
	}
}
