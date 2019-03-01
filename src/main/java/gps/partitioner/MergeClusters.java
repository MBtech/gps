package gps.partitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class MergeClusters {

	public static void main(String[] args) throws ParseException, IOException {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("ncif", "numberofclustersinfiles", true, "");
		options.addOption("nfc", "numberoffinalclusters", true, "");
		options.addOption("ne", "numberofedges", true, "");
		options.addOption("cfs", "clusterfiles", true, "");
		options.addOption("ifs", "inputfiles", true, "");
		options.addOption("oidfs", "originalidfiles", true, "");
		options.addOption("nns", "numberofnodes", true, "");
		options.addOption("od", "outputDirectory", true, "");
		options.addOption("csof", "clustersummaryoutputile", true, "");
		options.addOption("cmof", "clustermapoutputile", true, "");
		options.addOption("rof", "relabelingoutputfile", true, "");
		CommandLine commandLine = parser.parse(options, args);
		int numClustersInFiles = Integer.parseInt(commandLine.getOptionValue("ncif"));
		int numFinalClusters = Integer.parseInt(commandLine.getOptionValue("nfc"));
		int numEdges = Integer.parseInt(commandLine.getOptionValue("ne"));

		String inputFilesString = commandLine.getOptionValue("ifs");
		String[] inputFilesStringSplit = inputFilesString.split(":");

		String clusterFilesString = commandLine.getOptionValue("cfs");
		String[] clusterFilesStringSplit = clusterFilesString.split(":");

		String originalIdFilesString = commandLine.getOptionValue("oidfs");
		String[] originalIdFilesStringSplit = originalIdFilesString.split(":");

		String numNodesString = commandLine.getOptionValue("nns");
		String[] numNodesStringSplit = numNodesString.split(":");

		ClusterData root =
			new ClusterData(inputFilesStringSplit[0], clusterFilesStringSplit[0],
				Integer.parseInt(numNodesStringSplit[0]), numClustersInFiles, null);
		for (int i = 1; i < inputFilesStringSplit.length; ++i) {
			ClusterData clusterData =
				new ClusterData(inputFilesStringSplit[i], clusterFilesStringSplit[i],
					Integer.parseInt(numNodesStringSplit[i]), numClustersInFiles,
					originalIdFilesStringSplit[i - 1], null);
			int[] mapping = getMapping(clusterFilesStringSplit[i]);
			ClusterData currentClusterData = root;
			for (int j = 0; j < mapping.length - 1; ++j) {
				currentClusterData = currentClusterData.subClusterMap.get(mapping[j]);
			}
			currentClusterData.subClusterMap.put(mapping[mapping.length - 1], clusterData);
			clusterData.parent = currentClusterData;
		}

		root.parseGraph(numClustersInFiles);
		ArrayList<Set<LinkedList<Integer>>> clusters =
			new ArrayList<Set<LinkedList<Integer>>>(numFinalClusters);
		for (int i = 0; i < numFinalClusters; ++i) {
			clusters.add(i, new HashSet<LinkedList<Integer>>());
		}
		int[] latestClusterEdgeSizes = new int[numFinalClusters];
		while (root.hasUnMarkedCluster()) {
			System.out.println("Calling findMaxEdgeUnmarkedCluster");
			LinkedList<Integer> maxEdgeUnmarkedClusterRecursiveLocation =
				root.findMaxEdgeUnmarkedCluster();
			int leastFullClusterIndex = getMinIndex(latestClusterEdgeSizes);
			int clusterEdgeSize = root.getClusterEdgeSize(maxEdgeUnmarkedClusterRecursiveLocation);
			clusters.get(leastFullClusterIndex).add(maxEdgeUnmarkedClusterRecursiveLocation);
			markClusterAsUsed(root, maxEdgeUnmarkedClusterRecursiveLocation);
			latestClusterEdgeSizes[leastFullClusterIndex] += clusterEdgeSize;
		}

		for (int i = 0; i < latestClusterEdgeSizes.length; ++i) {
			System.out.println("Cluster " + i + " edgeSize: " + latestClusterEdgeSizes[i]);
		}

		for (int i = 0; i < numFinalClusters; ++i) {
			System.out.println("Start of Cluster " + i + " ------------------------------");
			Set<LinkedList<Integer>> cluster = clusters.get(i);

			for (LinkedList<Integer> recursiveLocation : cluster) {
				root.assignClusterToFinalCluster(recursiveLocation, i);
				if (recursiveLocation.isEmpty()) {
					System.out.println("RECURSIVE LOCATION IS EMPTY!!!!!");
				}
				for (Integer location : recursiveLocation) {
					System.out.print(" " + location);
				}
				System.out.println();
			}
			System.out.println("End of Cluster " + i + " ------------------------------");
		}
		root.printForDebugging();

		int[] finalClusterMap = new int[root.graphObj.graph.length];
		root.assignOriginalIdsToClusters(finalClusterMap);
		int counter = 0;
		for (int i = 1; i < finalClusterMap.length; ++i) {
			if (i == -1) {
				counter++;
				System.out.println(i + " is -1");
			}
		}
		System.out.println("Total -1s: " + counter);
		ClusterSummary finalClusterSummary =
			PartitionerUtils.getClusterSummary(root.graphObj, numFinalClusters, finalClusterMap);
		PartitionerUtils.smoothCluster(finalClusterSummary, numEdges, root.graphObj.graph);

		String clusterSummaryOutputFile =
			commandLine.getOptionValue("od") + "/" + commandLine.getOptionValue("csof");
		PartitionerUtils.writeClusterSummary(clusterSummaryOutputFile, numFinalClusters,
			root.graphObj.graph, finalClusterSummary);
		String clusterMapOutputFile =
			commandLine.getOptionValue("od") + "/" + commandLine.getOptionValue("cof");
		PartitionerUtils.writeClusterFile(finalClusterMap, clusterMapOutputFile);

		int[][] relabeledGraph =
			PartitionerUtils.getRelabeledGraph(finalClusterMap, root.graphObj.graph,
				numFinalClusters);
		String relabelingOutputFile =
			commandLine.getOptionValue("od") + "/" + commandLine.getOptionValue("rof");
		PartitionerUtils.writeGraphInSnapFormat(relabelingOutputFile + "-snap-format",
			relabeledGraph);
		PartitionerUtils.writeGraphInPartitions(relabeledGraph, relabelingOutputFile,
			numFinalClusters);
	}

	private static int getMinIndex(int[] latestClusterEdgeSizes) {
		int minIndex = 0;
		int minValue = latestClusterEdgeSizes[0];
		for (int i = 1; i < latestClusterEdgeSizes.length; ++i) {
			if (latestClusterEdgeSizes[i] < minValue) {
				minValue = latestClusterEdgeSizes[i];
				minIndex = i;
				System.out.println("Found a new minValue: " + minValue + " minIndex: " + i);
			}
		}
		return minIndex;
	}

	private static int[] getMapping(String fileString) {
		String[] split = fileString.split("cluster-");
		System.out.println("fileString: " + fileString);
		for (int i = 0; i < split.length; ++i) {
			System.out.println("split: " + split[i]);
		}

		int[] retVal = new int[split.length - 1];
		for (int i = 1; i < split.length; ++i) {
			if (i != split.length - 1) {
				retVal[i - 1] = Integer.parseInt(split[i].replace("-", ""));
			} else {
				retVal[i - 1] = Integer.parseInt("" + split[i].charAt(0));
			}
			System.out.println("putting " + retVal[i - 1] + " at index: " + (i - 1));
		}
		return retVal;
	}

	public static void markClusterAsUsed(ClusterData root, List<Integer> recursiveLocation) {
		ClusterData currentCluster = root;
		for (int i = 0; i < recursiveLocation.size() - 1; ++i) {
			int nextCluster = recursiveLocation.get(i);
			System.out.println("nextCluster: " + nextCluster);
			currentCluster = currentCluster.subClusterMap.get(nextCluster);
		}
		currentCluster.clustersUsed[recursiveLocation.get(recursiveLocation.size() - 1)] = true;
	}

	public static class ClusterData {
		String inputFile = null;
		String originalIdsFile = null;
		String clusterFile = null;
		int numNodes;
		Graph graphObj = null;
		ClusterSummary clusterSummary = null;
		boolean[] clustersUsed = null;
		int[] originalIds = null;
		Map<Integer, ClusterData> subClusterMap = new HashMap<Integer, ClusterData>();
		int numClusters;
		int[] finalClusters;
		ClusterData parent = null;

		public ClusterData(String inputFile, String clusterFile, int numNodes, int numClusters,
			ClusterData parent) {
			this.inputFile = inputFile;
			this.clusterFile = clusterFile;
			this.numNodes = numNodes;
			this.numClusters = numClusters;
			this.parent = parent;
			this.clustersUsed = new boolean[numClusters];
			finalClusters = new int[numClusters];
			for (int i = 0; i < numClusters; ++i) {
				clustersUsed[i] = false;
				finalClusters[i] = -1;
			}
		}

		public ClusterData(String inputFile, String clusterFile, int numNodes, int numClusters,
			String originalIdsFile, ClusterData parent) {
			this(inputFile, clusterFile, numNodes, numClusters, parent);
			this.originalIdsFile = originalIdsFile;
		}

		public void assignClusterToFinalCluster(List<Integer> recursiveClusterLocation,
			int finalClusterNo) {
			if (recursiveClusterLocation.size() == 1) {
				System.out.println("Assigning cluster " + recursiveClusterLocation.get(0)
					+ " to finalCluster: " + finalClusterNo);
				finalClusters[recursiveClusterLocation.get(0)] = finalClusterNo;
			} else {
				subClusterMap.get(recursiveClusterLocation.get(0)).assignClusterToFinalCluster(
					recursiveClusterLocation.subList(1, recursiveClusterLocation.size()),
					finalClusterNo);
			}
		}

		public void parseGraph(int numClusters) throws IOException {
			graphObj =
				PartitionerUtils.getGraph("/Users/semihsalihoglu/projects/GPS/data/" + inputFile,
					numNodes, true /* undirected */);
			clusterSummary =
				PartitionerUtils.getClusterSummary(
					"/Users/semihsalihoglu/projects/GPS/graclus-data/" + clusterFile, graphObj,
					numClusters);
			if (originalIdsFile != null) {
				originalIds =
					PartitionerUtils.readOriginalIdsFile("/Users/semihsalihoglu/projects/GPS/data/"
						+ originalIdsFile, (graphObj.graph.length - 1));
			}
			for (ClusterData subClusterData : subClusterMap.values()) {
				subClusterData.parseGraph(numClusters);
			}
		}

		public int getClusterEdgeSize(List<Integer> recursiveLocation) {
			if (recursiveLocation.size() == 1) {
				return clusterSummary.clusterEdgeSizes[recursiveLocation.get(0)];
			} else {
				Integer subClusterIndex = recursiveLocation.get(0);
				LinkedList<Integer> newRecursiveLocation =
					new LinkedList<Integer>(recursiveLocation);
				newRecursiveLocation.remove(0);
				return subClusterMap.get(subClusterIndex).getClusterEdgeSize(newRecursiveLocation);
			}
		}

		public boolean hasUnMarkedCluster() {
			for (ClusterData subCluster : subClusterMap.values()) {
				if (subCluster.hasUnMarkedCluster()) {
					System.out.println("SubCluster has an unmarked cluster!");
					return true;
				}
			}
			for (int i = 0; i < clustersUsed.length; ++i) {
				if (!clustersUsed[i] && !subClusterMap.containsKey(i)) {
					System.out.println("Cluster " + i + " is not yet marked!");
					return true;
				}
			}
			return false;
		}

		// Does not include clusters that have subclusters in it.
		public LinkedList<Integer> findMaxEdgeUnmarkedCluster() {
			LinkedList<Integer> currentRecursiveLocation = null;
			int currentMaxEdgeSize = -1;
			for (int subClusterKey : subClusterMap.keySet()) {
				ClusterData subClusterData = subClusterMap.get(subClusterKey);
				System.out.println("Looking for max edge unmarked cluster in cluster: "
					+ subClusterKey);
				LinkedList<Integer> returnedMaxEdgeLocation =
					subClusterData.findMaxEdgeUnmarkedCluster();
				if (returnedMaxEdgeLocation == null) {
					System.out.println("Subcluster: " + subClusterKey + " returned null");
					continue;
				}
				ClusterData currentClusterData = subClusterData;
				for (int i = 0; i < returnedMaxEdgeLocation.size() - 1; ++i) {
					currentClusterData =
						currentClusterData.subClusterMap.get(returnedMaxEdgeLocation.get(i));
				}
				if (currentClusterData.clusterSummary.clusterEdgeSizes[returnedMaxEdgeLocation
					.get(returnedMaxEdgeLocation.size() - 1)] > currentMaxEdgeSize) {
					currentMaxEdgeSize =
						currentClusterData.clusterSummary.clusterEdgeSizes[returnedMaxEdgeLocation
							.get(returnedMaxEdgeLocation.size() - 1)];
					System.out.println("Found a new max edge unmarked cluster: "
						+ currentMaxEdgeSize);
					currentRecursiveLocation = returnedMaxEdgeLocation;
					currentRecursiveLocation.addFirst(subClusterKey);
				}
			}

			for (int i = 0; i < clusterSummary.clusterEdgeSizes.length; ++i) {
				if (subClusterMap.containsKey(i)) {
					System.out.println("skipping subgraph: " + i);
					continue;
				}
				System.out.println("i: " + i + " currentMaxEdgeSize: " + currentMaxEdgeSize
					+ " isUsed: " + clustersUsed[i]);
				if (!clustersUsed[i] && (clusterSummary.clusterEdgeSizes[i] > currentMaxEdgeSize)) {
					currentMaxEdgeSize = clusterSummary.clusterEdgeSizes[i];
					currentRecursiveLocation = new LinkedList<Integer>();
					currentRecursiveLocation.add(i);
				}
			}
			if (currentRecursiveLocation == null) {
				System.out.println("All Clusters are marked!");
			} else {
				System.out.println("Found maxEdgeSizeClusterSize: " + currentMaxEdgeSize);
				System.out.print("Location:");
				for (int i = 0; i < currentRecursiveLocation.size(); ++i) {
					System.out.print(" " + currentRecursiveLocation.get(i));
				}
				System.out.println();
			}
			return currentRecursiveLocation;
		}

		public void assignOriginalIdsToClusters(int[] finalClusterMap) {
			int[] clusterMap = clusterSummary.clusterMap;
			int totalUnassignedNodes = 0;
			for (int i = 1; i < graphObj.graph.length; ++i) {
				if (finalClusters[clusterMap[i]] == -1) {
					totalUnassignedNodes++;
				} else {
					finalClusterMap[findOriginalId(i)] = finalClusters[clusterMap[i]];
				}
			}
			System.out.println("TotalUnassignedNodes: " + totalUnassignedNodes);
			for (ClusterData subCluster : subClusterMap.values()) {
				subCluster.assignOriginalIdsToClusters(finalClusterMap);
			}
		}

		public int findOriginalId(int mappedId) {
			if (parent == null) {
				return mappedId;
			} else {
				return parent.findOriginalId(originalIds[mappedId]);
			}
		}

		public void printForDebugging() {
			System.out.println("inputFile: " + inputFile);
			System.out.println("originalIdFiles: " + originalIdsFile);
			System.out.println("clusterFile: " + clusterFile);
			System.out.println("numNodes: " + numNodes);
			System.out.println("Starting FinalClusters -----------------");
			for (int i = 0; i < finalClusters.length; ++i) {
				System.out.println("cluster " + i + " -> " + finalClusters[i]);
			}
			System.out.println("Ending FinalClusters -----------------");
			if (graphObj != null) {
				System.out.println("numEdges: " + graphObj.numEdges);
				System.out.println("isUndirected: " + graphObj.isUndirected);
				System.out.println("clusterSummary: " + clusterSummary.toDebugString());
			}
			for (ClusterData subClusterData : subClusterMap.values()) {
				subClusterData.printForDebugging();
			}

		}
	}
}
