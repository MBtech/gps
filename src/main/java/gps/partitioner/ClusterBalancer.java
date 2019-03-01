package gps.partitioner;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class ClusterBalancer {

	public static void main(String[] args) throws ParseException, NumberFormatException,
		IOException {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("nc", "numberofclusters", true, "");
		options.addOption("cf", "clusterfile", true, "");
		options.addOption("if", "inputfile", true, "");
		options.addOption("nn", "numberOfNodes", true, "");
		options.addOption("ne", "numberOfEdges", true, "");
		options.addOption("od", "outputdirectory", true, "");
		options.addOption("of", "outputfile", true, "");
		CommandLine commandLine = parser.parse(options, args);
		int numberOfClusters = Integer.parseInt(commandLine.getOptionValue("nc"));
		int numNodes = Integer.parseInt(commandLine.getOptionValue("nn"));
		int numEdges = Integer.parseInt(commandLine.getOptionValue("ne"));

		System.out.println("numNodes: " + numNodes);
		long startTime = System.currentTimeMillis();
		String inputFile = commandLine.getOptionValue("if");
		Graph graphObj = PartitionerUtils.getGraph(inputFile, numNodes, false /* directed */);
		String clusterFile = commandLine.getOptionValue("cf");
		int[] clusterMap = PartitionerUtils.getClusterMap(clusterFile, numNodes);
		int[][] graph = graphObj.graph;

		int[] currentClusterNodeSizes = new int[numberOfClusters];
		int[] currentClusterEdgeSizes = new int[numberOfClusters];
		int[] newClusterMap = new int[numNodes + 1];
		int clusterEdgeTreshold = (numEdges / numberOfClusters) + 10000;
		for (int i = 1; i < graph.length; ++i) {
			int clusterOfI = clusterMap[i];
			if (currentClusterEdgeSizes[clusterOfI] <= clusterEdgeTreshold) {
				newClusterMap[i] = clusterOfI;
				currentClusterNodeSizes[clusterOfI]++;
				currentClusterEdgeSizes[clusterOfI] += graph[i].length;
			} else {
				newClusterMap[i] = -1;
			}
		}

		int overlayCounter = 0;
		for (int i = 1; i < graph.length; ++i) {
			if (newClusterMap[i] == -1) {
				if (currentClusterEdgeSizes[overlayCounter] > clusterEdgeTreshold) {
					overlayCounter =
						findNewOverLayCounter(currentClusterEdgeSizes, clusterEdgeTreshold);
				}
				newClusterMap[i] = overlayCounter;
				currentClusterNodeSizes[overlayCounter]++;
				currentClusterEdgeSizes[overlayCounter] += graph[i].length;
			}
		}

		for (int i = 0; i < currentClusterNodeSizes.length; ++i) {
			System.out.println("CurrentCluster: " + i + " nodeSize: " + currentClusterNodeSizes[i]
				+ " edgeSize: " + currentClusterEdgeSizes[i]);
		}
		String outputFileName =
			commandLine.getOptionValue("od") + "/" + commandLine.getOptionValue("of");
		PartitionerUtils.writeClusterFile(outputFileName, newClusterMap);
		long endTime = System.currentTimeMillis();
		System.out.println("TotalTime: " + (endTime - startTime));
	}

	private static int findNewOverLayCounter(int[] currentClusterSizes, int clusterTreshold) {
		for (int i = 0; i < currentClusterSizes.length; ++i) {
			if (currentClusterSizes[i] <= clusterTreshold) {
				System.out.println("Found a new overlayCounter: " + i);
				return i;
			}
		}
		System.err.println("Could not find an overlay counter!!!!");
		return -1;
	}
}
