package gps.partitioner;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class WriteGraphClusterAsSubgraph {

	public static void main(String[] args) throws ParseException, IOException {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("nc", "numberofclusters", true, "");
		options.addOption("cf", "clusterfile", true, "");
		options.addOption("nn", "numberofnodes", true, "");
		options.addOption("if", "inputfile", true, "");
		options.addOption("od", "outputDirectory", true, "");
		options.addOption("ofp", "outputfileprefix", true, "");
		options.addOption("cid", "clusterid", true, "");
		CommandLine commandLine = parser.parse(options, args);
		int numClusters = Integer.parseInt(commandLine.getOptionValue("nc"));
		int numNodes = Integer.parseInt(commandLine.getOptionValue("nn"));

		Graph[] graphObjs = PartitionerUtils.getGraphs(commandLine.getOptionValue("if"), numNodes);
		Graph graphObjUndirected = graphObjs[0];

		int[][] graph = graphObjUndirected.graph;
		ClusterSummary clusterSummaryUndirected =
			PartitionerUtils.getClusterSummary(commandLine.getOptionValue("cf"),
				graphObjUndirected, numClusters);
		int[] clusterMap = clusterSummaryUndirected.clusterMap;
		int[] clusterEdgeSizes = clusterSummaryUndirected.clusterEdgeSizes;
		int clusterIndex = -1;
		if (commandLine.hasOption("cid")) {
			clusterIndex = Integer.parseInt(commandLine.getOptionValue("cid"));
		} else {
			clusterIndex = findMax(clusterEdgeSizes);
		}
		HashSet<Integer> subgraphNodeIds = new HashSet<Integer>();
		for (int i = 1; i < graph.length; ++i) {
			if (clusterMap[i] == clusterIndex) {
				subgraphNodeIds.add(i);
			}
		}
		Graph subgraphObjUndirected =
			PartitionerUtils.getSubgraph(graphObjUndirected, subgraphNodeIds);
		String outputFileName =
			commandLine.getOptionValue("od") + "/" + commandLine.getOptionValue("ofp")
				+ "-snap-format";
		PartitionerUtils.writeGraphInGraclusFormat(outputFileName + "-graclus-format",
			subgraphObjUndirected);
		String originalIdsOutputFile =
			commandLine.getOptionValue("od") + "/" + commandLine.getOptionValue("ofp")
				+ "-originalIds";
		PartitionerUtils.writeOriginalIdsFile(originalIdsOutputFile,
			subgraphObjUndirected.originalIds);

		Graph graphObjDirected = graphObjs[1];
		Graph subgraphObjDirected = PartitionerUtils.getSubgraph(graphObjDirected, subgraphNodeIds);
		PartitionerUtils.writeGraphInSnapFormat(outputFileName + "-snap-format",
			subgraphObjDirected.graph);
	}

	private static int findMax(int[] intArray) {
		int maxId = 0;
		for (int i = 1; i < intArray.length; ++i) {
			if (intArray[i] > intArray[maxId]) {
				maxId = i;
			}
		}
		return maxId;
	}
}
