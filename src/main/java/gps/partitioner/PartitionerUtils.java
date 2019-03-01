package gps.partitioner;

import gps.node.Pair;
import gps.partitioner.SequentialPregel.State;
import gps.partitioner.SequentialPregelRandomCoarsening.RandomCoarseningMessage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PartitionerUtils {

	private static final String DOT_ORIGINALCLUSTERIDS = ".originalclusterids";
	private static final String DOT_RELABELSMAP = ".relabelsmap";

	public static String getOutputFileName(CommandLine commandLine) {
		return commandLine.getOptionValue("od") + "/" + commandLine.getOptionValue("of");
	}

	public static void writeClusterSummary(String outputFile, int numClusters, int[][] graph,
		ClusterSummary clusterSummary) throws IOException {
		int[] clusterMap = clusterSummary.clusterMap;
		int[] clusterNodeSizes = clusterSummary.clusterNodeSizes;
		int[] clusterEdgeSizes = clusterSummary.clusterEdgeSizes;
		int[] numMessagesPerCluster =
			PartitionerUtils.countTotalNumMessagesSent(numClusters, clusterMap, graph);
		int totalNodes = 0;
		int totalEdges = 0;
		int totalMessages = 0;
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile));
		for (int i = 0; i < numClusters; ++i) {
			totalNodes += clusterNodeSizes[i];
			totalEdges += clusterEdgeSizes[i];
			totalMessages += numMessagesPerCluster[i];
			System.out.println("Printing cluster: " + i);
			String outputString =
				"cluster " + i + " nodeSize: " + clusterNodeSizes[i] + " edgeSize: "
					+ clusterEdgeSizes[i] + " edgeDensity: "
					+ (clusterEdgeSizes[i] / clusterNodeSizes[i]) + " totalOutgoingMessages: "
					+ numMessagesPerCluster[i];
			bufferedWriter.write(outputString + "\n");
			System.out.println(outputString);
		}
		bufferedWriter.close();
		System.out.println("TotalNodes: " + totalNodes);
		System.out.println("TotalEdges: " + totalEdges);
		System.out.println("TotalMessages: " + totalMessages);
	}

	public static ClusterSummary getClusterSummary(String clusterFile, Graph graphObj,
		int numClusters) throws NumberFormatException, IOException {
		int[] clusterMap = PartitionerUtils.getClusterMap(clusterFile, (graphObj.graph.length - 1));
		return getClusterSummary(graphObj, numClusters, clusterMap);
	}

	public static int[][] getRelabeledGraph(int[] clusterMap, int[][] graph, int numClusters)
		throws IOException {
		int[] newIds = getNewIds(clusterMap, numClusters);
		int maxId = findMaxId(newIds, numClusters);
		int[][] relabeledGraph = new int[maxId + 1][];

		for (int i = 1; i < graph.length; ++i) {
			int newId = newIds[i];
			relabeledGraph[newId] = new int[graph[i].length];
			for (int j = 0; j < graph[i].length; ++j) {
				relabeledGraph[newId][j] = newIds[graph[i][j]];
			}
		}
		return relabeledGraph;
	}

	private static int findMaxId(int[] newIds, int numClusters) {
		int maxId = newIds[newIds.length - numClusters];
		for (int i = newIds.length - numClusters + 1; i < newIds.length; ++i) {
			if (maxId < newIds[i]) {
				maxId = newIds[i];
			}
		}
		return maxId;
	}

	private static int[] getNewIds(int[] clusterMap, int numClusters) {
		int[] nextIds = new int[numClusters];
		nextIds[0] = numClusters;
		for (int i = 1; i < numClusters; ++i) {
			nextIds[i] = i;
		}
		int[] newIds = new int[clusterMap.length];
		for (int i = 1; i < clusterMap.length; ++i) {
			int clusterOfOldId = clusterMap[i];
			newIds[i] = nextIds[clusterOfOldId];
			nextIds[clusterOfOldId] += numClusters;
		}
		return newIds;
	}

	public static void writeClusterFile(int[] clusterMap, String outputFile) throws IOException {
		BufferedWriter bufferedWriter = getBufferedWriter(outputFile);
		for (int i = 1; i < clusterMap.length; ++i) {
			bufferedWriter.write(clusterMap[i] + "\n");
		}
		bufferedWriter.close();
	}

	public static BufferedWriter getBufferedWriter(String outputFile) throws IOException {
		return new BufferedWriter(new FileWriter(outputFile));
	}

	public static void smoothCluster(ClusterSummary finalClusterSummary, int numEdges, int[][] graph) {
		System.out.println("Starting smooting clustering...");
		int[] clusterMap = finalClusterSummary.clusterMap;
		int averageNumberOfEdgesPerCluster =
			(numEdges / finalClusterSummary.clusterEdgeSizes.length) + 500;
		int[] clusterEdgeSizes = finalClusterSummary.clusterEdgeSizes;
		int[] clusterNodeSizes = finalClusterSummary.clusterNodeSizes;
		int leastLoadedClusterId = getLeastLoadedClusterId(clusterEdgeSizes);
		int maxLoadedClusterId = getMaxLoadedClusterId(clusterEdgeSizes);
		while (clusterEdgeSizes[leastLoadedClusterId] < averageNumberOfEdgesPerCluster
			&& clusterEdgeSizes[maxLoadedClusterId] > averageNumberOfEdgesPerCluster) {
			System.out.println("leastLoadedClusterId: " + leastLoadedClusterId + " clusterSize: "
				+ clusterEdgeSizes[leastLoadedClusterId]);
			System.out.println("maxLoadedClusterId: " + maxLoadedClusterId + " clusterSize: "
				+ clusterEdgeSizes[maxLoadedClusterId]);
			List<Integer> leastLoadedClusterNodeIds =
				finalClusterSummary.clusters.get(maxLoadedClusterId);
			List<Integer> maxLoadedClusterNodeIds =
				finalClusterSummary.clusters.get(maxLoadedClusterId);
			int numNodesSmoothed = 0;
			int numEdgesSmoothed = 0;
			while (!maxLoadedClusterNodeIds.isEmpty()
				&& clusterEdgeSizes[leastLoadedClusterId] < averageNumberOfEdgesPerCluster
				&& clusterEdgeSizes[maxLoadedClusterId] > averageNumberOfEdgesPerCluster) {
				numNodesSmoothed++;
				int smoothedNodeId = maxLoadedClusterNodeIds.remove(0);
				// Assigning node id to new cluster.
				clusterMap[smoothedNodeId] = leastLoadedClusterId;
				leastLoadedClusterNodeIds.add(smoothedNodeId);
				int numNeighborsOfSmoothedNodeId = graph[smoothedNodeId].length;
				numEdgesSmoothed += numNeighborsOfSmoothedNodeId;
				leastLoadedClusterNodeIds.add(smoothedNodeId);
				clusterEdgeSizes[leastLoadedClusterId] += numNeighborsOfSmoothedNodeId;
				clusterEdgeSizes[maxLoadedClusterId] -= numNeighborsOfSmoothedNodeId;
				clusterNodeSizes[leastLoadedClusterId]++;
				clusterNodeSizes[maxLoadedClusterId]--;
			}
			System.out.println("Put " + numNodesSmoothed + " nodes from cluster: "
				+ maxLoadedClusterId + " to cluster: " + leastLoadedClusterId);
			System.out.println("Put " + numEdgesSmoothed + " edges from cluster: "
				+ maxLoadedClusterId + " to cluster: " + leastLoadedClusterId);
			leastLoadedClusterId = getLeastLoadedClusterId(clusterEdgeSizes);
			maxLoadedClusterId = getMaxLoadedClusterId(clusterEdgeSizes);
		}
	}

	private static int getLeastLoadedClusterId(int[] clusterEdgeSizes) {
		int minIndex = 0;
		int minValue = clusterEdgeSizes[0];
		for (int i = 1; i < clusterEdgeSizes.length; ++i) {
			if (clusterEdgeSizes[i] < minValue) {
				minValue = clusterEdgeSizes[i];
				minIndex = i;
			}
		}
		return minIndex;
	}

	private static int getMaxLoadedClusterId(int[] clusterEdgeSizes) {
		int maxIndex = 0;
		int maxValue = clusterEdgeSizes[0];
		for (int i = 1; i < clusterEdgeSizes.length; ++i) {
			if (clusterEdgeSizes[i] > maxValue) {
				maxValue = clusterEdgeSizes[i];
				maxIndex = i;
			}
		}
		return maxIndex;
	}

	public static ClusterSummary getClusterSummary(Graph graphObj, int numClusters, int[] clusterMap) {
		int[] clusterNodeSizes = new int[numClusters];
		int[] clusterEdgeSizes = new int[numClusters];

		for (int i = 1; i < graphObj.graph.length; ++i) {
			if (i % 1000000 == 0) {
				System.out.println("getting cluster summary. i: " + i);
			}
			clusterNodeSizes[clusterMap[i]]++;
			clusterEdgeSizes[clusterMap[i]] += graphObj.graph[i].length;
		}
		return new ClusterSummary(clusterMap, clusterNodeSizes, clusterEdgeSizes);
	}

	public static void writeOriginalIdsFile(String originalIdsFile, int[] originalIds)
		throws IOException {
		BufferedWriter bufferedWriter =
			new BufferedWriter(new FileWriter(originalIdsFile), 10000000);
		for (int i = 1; i < originalIds.length; ++i) {
			bufferedWriter.write(i + " " + originalIds[i] + "\n");
		}
		bufferedWriter.close();
	}
	
	public static void countNumEdgesCrossingPartitions(String inputFile, int numPartitions)
		throws IOException {
		BufferedReader bufferedReader = getBufferedReader(inputFile);
		String line = null;
		int counter = 0;
		int numEdgesCrossingPartitions = 0;
		long previousTime = System.currentTimeMillis();
		String[] split;
		while ((line = bufferedReader.readLine()) != null) {
			if (counter++ % 10000000 == 0) {
				System.out.println("Finished " + counter + " lines. Time taken: "
					+ (System.currentTimeMillis() - previousTime) 
					+ " numEdgesCrossingPartitions: " + numEdgesCrossingPartitions);
				previousTime = System.currentTimeMillis();
			}
			split = line.split("\\s+");
			if (((Integer.parseInt(split[0]) - Integer.parseInt(split[1])) % numPartitions) != 0) {
				numEdgesCrossingPartitions++;
			}
		}
		bufferedReader.close();
	}

	public static void writeMETISRelabelsMap(String metisPartsOutput, int numPartitions,
		String outputFile) throws IOException {
		BufferedReader bufferedReader = getBufferedReader(metisPartsOutput);
		int partitionId = 0;
		String line = null;
		int[] nextIdsToGive = new int[numPartitions];
		for (int i = 0; i < numPartitions; ++i) {
			nextIdsToGive[i] = i;
		}
		BufferedWriter bufferedWriter = getBufferedWriter(outputFile);
		int counter = 0;
		long previousTime = System.currentTimeMillis();
		while ((line = bufferedReader.readLine()) != null) {
			partitionId = Integer.parseInt(line);
			bufferedWriter.write("-1 ");
			bufferedWriter.write(counter++ + " ");
			bufferedWriter.write(nextIdsToGive[partitionId] + "\n");
			nextIdsToGive[partitionId] += numPartitions;
			if (counter % 10000000 == 0) {
				System.out.println("Finished " + counter + " lines. Time taken: "
					+ (System.currentTimeMillis() - previousTime));
				previousTime = System.currentTimeMillis();
			}
		}
		bufferedReader.close();
		bufferedWriter.close();
	}

	public static int[] readOriginalIdsFile(String originalIdsFile, int numNodes)
		throws IOException {
		System.out.println("Reading originalIdsFile: " + originalIdsFile);
		BufferedReader bufferedReader = getBufferedReader(originalIdsFile);
		int counter = 0;
		String line = null;
		String[] split = null;
		int[] originalIds = new int[numNodes + 1];
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			split = line.split("\\s+");
			if (counter % 1000000 == 0) {
				System.out.println("Parsed " + counter + " lines. Time: ");
			}
			originalIds[Integer.parseInt(split[0])] = Integer.parseInt(split[1]);
		}
		bufferedReader.close();
		return originalIds;
	}

	public static void writeGraphInSnapFormat(String outputFile, int[][] graph) throws IOException {
		System.out.println("WritingGraphInSnapFormat. ");
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile), 10000000);
		for (int i = 1; i < graph.length; ++i) {
			if (graph[i] == null) {
				continue;
			}
			for (int k = 0; k < graph[i].length; ++k) {
				bufferedWriter.write(i + " " + graph[i][k] + " 1\n");
			}

		}
		bufferedWriter.close();
	}

	// This method writes the graph into numClusters partitions according to % function
	public static void writeGraphInPartitions(int[][] graph, String outputFilePrefix,
		int numPartitions) throws IOException {
		long startTime = System.currentTimeMillis();
		System.out.println("WritingGraphInPartitions. numPartitions: " + numPartitions);

		List<BufferedWriter> bufferedWriterList = new ArrayList<BufferedWriter>();
		for (int i = 0; i < numPartitions; ++i) {
			String outputFileName = outputFilePrefix + "-partition-" + i + "-of-" + numPartitions;
			System.out.println(outputFileName);
			bufferedWriterList.add(new BufferedWriter(new FileWriter(outputFileName)));
		}
		for (int nodeId = 1; nodeId < graph.length; ++nodeId) {
			if (graph[nodeId] == null) {
				continue;
			}
			int index = nodeId % numPartitions;
			BufferedWriter bufferedWriter = bufferedWriterList.get(index);
			bufferedWriter.write("" + nodeId);
			for (int neighborId : graph[nodeId]) {
				bufferedWriter.write(" " + neighborId);
			}
			bufferedWriter.write("\n");
		}
		for (BufferedWriter bufferedWriter : bufferedWriterList) {
			bufferedWriter.flush();
			bufferedWriter.close();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("TimeTaken: " + (endTime - startTime));
	}

	public static void writeGraphInGraclusFormat(String outputFile, Graph graphObj)
		throws IOException {
		System.out.println("WritingGraphInGraclusOutput. ");
		assert graphObj.isUndirected;
		int[][] graph = graphObj.graph;
		int numNodes = graph.length - 1;
		int numEdges = graphObj.numEdges;
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile), 10000000);
		bufferedWriter.write(numNodes + " " + (numEdges / 2) + "\n");
		String edges = "";
		// int[] allPossibleEdges = new int[graph.length];
		HashSet<Integer> cleanNumEdges = new HashSet<Integer>();
		for (int i = 1; i < graph.length; ++i) {

			edges = "";
			// long timeBeforeAddingIntoSet = System.currentTimeMillis();
			for (int k = 0; k < graph[i].length; ++k) {
				// allPossibleEdges[graph[i][k]] = 1;
				cleanNumEdges.add(graph[i][k]);
			}
			// long timeAfterAddingIntoSet = System.currentTimeMillis();
			// for (int t = 1; t < allPossibleEdges.length; ++t) {
			// if (allPossibleEdges[t] == 1) {
			// edges += " "
			//
			// }
			for (int j : cleanNumEdges) {
				edges += " " + j;
			}
			if (i % 500000 == 0) {
				System.out.println("Writing " + i + " lines");
				System.out.println("numEdges of node " + i + " is: " + graph[i].length);
				// System.out.println("TimeTakenToProcess: " + (timeAfterAddingIntoSet -
				// timeBeforeAddingIntoSet));
			}
			// allPossibleEdges = new int[graph.length];
			cleanNumEdges.clear();
			bufferedWriter.write(edges.trim() + "\n");
		}
		bufferedWriter.close();
	}

	public static Graph getSubgraph(Graph graph, HashSet<Integer> subgraphNodeIds) {
		int subgraphSize = subgraphNodeIds.size();
		System.out.println("Getting subgraph. GraphSize: " + graph.graph.length + " SubgraphSize: "
			+ subgraphNodeIds.size());
		int[][] subgraph = new int[subgraphSize + 1][];
		int[] originalIds = new int[subgraphSize + 1];
		int[] subgraphIds = new int[graph.graph.length + 1];
		int[][] originalGraph = graph.graph;
		int lastGivenSubgraphId = 1;
		for (int originalGraphId : subgraphNodeIds) {
			int subgraphId = lastGivenSubgraphId++;
			originalIds[subgraphId] = originalGraphId;
			subgraphIds[originalGraphId] = subgraphId;
		}

		HashSet<Integer> neighborSubgraphIds = new HashSet<Integer>();
		int numEdges = 0;
		for (int subgraphNodeId = 1; subgraphNodeId < (subgraphSize + 1); ++subgraphNodeId) {
			if (subgraphNodeId % 200000 == 0) {
				System.out.println("Iterating over subgraphNodeId: " + subgraphNodeId);
			}
			int originalGraphId = originalIds[subgraphNodeId];
			int numNeighbors = 0;
			for (int originalNeighborId : originalGraph[originalGraphId]) {
				if (subgraphIds[originalNeighborId] > 0) {
					numNeighbors++;
					neighborSubgraphIds.add(subgraphIds[originalNeighborId]);
				}
			}
			int[] neighborsArray = new int[neighborSubgraphIds.size()];
			numEdges += neighborSubgraphIds.size();
			int counter = 0;
			for (int neighborSubgraphId : neighborSubgraphIds) {
				neighborsArray[counter++] = neighborSubgraphId;
			}
			subgraph[subgraphNodeId] = neighborsArray;
			neighborSubgraphIds.clear();
		}
		return new Graph(numEdges, subgraph, graph.isUndirected, originalIds);
	}

	public static int[] countTotalNumMessagesSent(int numberOfClusters, int[] clusterMap,
		int[][] graph) {
		int[] numMessagesPerCluster = new int[numberOfClusters];
		for (int i = 1; i < graph.length; ++i) {
			int clusterOfI = clusterMap[i];
			for (int j = 0; j < graph[i].length; ++j) {
				if (clusterOfI != clusterMap[graph[i][j]]) {
					numMessagesPerCluster[clusterOfI]++;
				}
			}
		}
		return numMessagesPerCluster;
	}

	public static void writeClusterFile(String outputClusterFile, int[] clusterMap)
		throws IOException {
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputClusterFile));
		for (int i = 1; i < clusterMap.length; ++i) {
			bufferedWriter.write("" + clusterMap[i] + "\n");
		}
		bufferedWriter.close();
	}

	public static int[] getClusterMap(String clusterFile, int numNodes)
		throws NumberFormatException, IOException {
		BufferedReader bufferedReader = getBufferedReader(clusterFile);
		String line = null;
		int[] clusters = new int[numNodes + 1];
		int counter = 1;
		while ((line = bufferedReader.readLine()) != null) {
			clusters[counter++] = Integer.parseInt("" + line.charAt(0));
		}
		return clusters;
	}

	public static int[] findClusterSizes(String inputFile, int numberOfClusters)
		throws NumberFormatException, IOException {
		BufferedReader bufferedReader = getBufferedReader(inputFile);
		String line = null;
		int[] clusterSizes = new int[numberOfClusters];
		while ((line = bufferedReader.readLine()) != null) {
			clusterSizes[Integer.parseInt("" + line.charAt(0))]++;
		}
		return clusterSizes;
	}

	public static int[] getEdgeCount(String inputFile, int numNodes, boolean undirected)
		throws IOException {
		BufferedReader bufferedReader = getBufferedReader(inputFile);
		String line;
		int counter = 0;
		String[] split = null;
		int[] edgeCount = new int[numNodes + 1];
		long previousTime = System.currentTimeMillis();
		int numEdges = 0;
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			split = line.split("\\s+");
			if (counter % 1000000 == 0) {
				long currentTime = System.currentTimeMillis();
				System.out.println("EdgeCounter " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			try {
				numEdges++;
				edgeCount[Integer.parseInt(split[0])]++;
				if (undirected) {
					numEdges++;
					edgeCount[Integer.parseInt(split[1])]++;
				}
			} catch (NumberFormatException e) {
			}
		}

		System.out.println("numEdges: " + numEdges);
		bufferedReader.close();
		return edgeCount;
	}

	public static Graph[] getGraphs(String inputFile, int numNodes) throws IOException {
		return getGraphs(inputFile, numNodes, true, true /* both directed and undirected */);
	}

	public static Graph getGraph(String inputFile, int numNodes, boolean isUndirected)
		throws IOException {
		return getGraphs(inputFile, numNodes, isUndirected, false /*
																 * is not both directed and
																 * undirected
																 */)[0];
	}
	
	private static void fillIncomingEdgesAndDegrees(String inputDirectedGraphFile,
		int[] numIncomingEdges, int[] numDegrees) throws NumberFormatException,
		IOException {
		System.out.println("Starting fillIncomingEdgesAndDegrees... inputFile: " + inputDirectedGraphFile);
		BufferedReader bufferedReader = getBufferedReader(inputDirectedGraphFile);
		String line = null;
		int counter = 0;
		String[] split = null;
		long previousTime = System.currentTimeMillis();
		int vertexId;
		int neighborId;
		long numEdges = 0;
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			split = line.split("\\s+");
			if (counter % 1000000 == 0) {
				long currentTime = System.currentTimeMillis();
				System.out.println("Parsed " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			vertexId = Integer.parseInt(split[0]) + 1;
			numEdges += split.length - 1;
			numDegrees[vertexId] += split.length - 1;
			for (int i = 1; i < split.length; ++i) {
				neighborId = Integer.parseInt(split[i]) + 1;
				numIncomingEdges[neighborId]++;
			}
		}
		bufferedReader.close();
		System.out.println("numDirectedEdges: " + numEdges);
		System.out.println("End of fillIncomingEdgesAndDegrees...");
	}

	public static void writeNormalizedGraphInMETISFormat(String inputFile, int numVertices,
		String outputFile, String inputDirectedGraph, boolean withVertexDegreeWeights,
		boolean withVertexSizeWeights) throws IOException {
		System.out.println("Running writeNormalizedGraphInMETISFormat xxxzz...");
		int[] numIncomingEdges = new int[numVertices + 1];
		int[] numDegrees = new int[numVertices + 1];
		if (withVertexDegreeWeights || withVertexSizeWeights) {
			fillIncomingEdgesAndDegrees(inputDirectedGraph, numIncomingEdges, numDegrees);
		}
		BufferedReader bufferedReader = getBufferedReader(inputFile);
		String line = null;
		int counter = 0;
		String[] split = null;
//		int[][] graph = new int[numVertices + 1][];
		List<List<Integer>> graph = new ArrayList<List<Integer>>(numVertices + 1);
		for (int i = 0; i < numVertices+1; ++i) {
			graph.add(new ArrayList<Integer>());
		}
		long previousTime = System.currentTimeMillis();
//		int[] tmpAdjList = null;
		List<Integer> tmpIntList = null;
		int vertexId;
		int neighborId;
		long numEdges = 0;
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			split = line.split("\\s+");
			if (counter % 1000000 == 0) {
				long currentTime = System.currentTimeMillis();
				System.out.println("Parsed " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			vertexId = Integer.parseInt(split[0]) + 1;
			tmpIntList = graph.get(vertexId);
//			tmpAdjList = new int[split.length - 1];
//			numEdges += split.length - 1;
			for (int i = 1; i < split.length; ++i) {
				neighborId = Integer.parseInt(split[i]) + 1;
				if (vertexId != neighborId) {
//					System.out.println("Found a self loop. veretxId: " + vertexId);
//				} else {
//					tmpAdjList[i-1] = neighborId;
					tmpIntList.add(neighborId);
//					numIncomingEdges[neighborId]++;
					numEdges++;
					
				}
			}
			if (vertexId >= graph.size()) {
				System.out.println("vertexId: " + vertexId + " is larger than the graph size!!!! Skipping but METIS won't work!!!!");
			}
//			else {
//				if (graph[vertexId] != null) {
//					System.out.println("This vertexId: "
//						+ vertexId + " has already ben assigned an adjacency list!!");
//				}
//				graph[vertexId] = tmpAdjList;
//			}
		}
		bufferedReader.close();
		System.out.println("numEdges: " + numEdges);
		System.out.println("numEdges/2: " + (numEdges/2));
		
		BufferedWriter bufferedWriter = getBufferedWriter(outputFile);
		bufferedWriter.write(numVertices + " " + (numEdges/2));
		if (withVertexDegreeWeights && withVertexSizeWeights) {
			bufferedWriter.write(" 010 3\n");
		} else if (withVertexDegreeWeights || withVertexSizeWeights) {
			bufferedWriter.write(" 010 2\n");
		} else {
			bufferedWriter.write("\n");
		}
		tmpIntList = null;
		for (int i = 1; i <= numVertices; ++i) {
			tmpIntList = graph.get(i); 
			// TODO(semih): Don't forget about the space in the beginning of the line!!
//			bufferedWriter.write(" " + numIncomingEdges[i] + " " + numDegrees[i] + " " + "1");
			if (withVertexDegreeWeights && withVertexSizeWeights) {
				bufferedWriter.write(" " + numIncomingEdges[i] + " " + numDegrees[i] + " " + "1");
			} else if (withVertexDegreeWeights) {
				bufferedWriter.write(" " + numDegrees[i] + " " + "1");
			} else if (withVertexSizeWeights) {
				bufferedWriter.write(" " + numIncomingEdges[i] + " " + "1");				
			}
			for (int neighborId2 : tmpIntList) {
				bufferedWriter.write(" " + neighborId2);
			}
			//			if (tmpAdjList != null) {
//				for (int j = 0; j < tmpAdjList.length; ++j) {
//					bufferedWriter.write(" " + tmpAdjList[j]);
//				}
//			} else {
//				System.out.println("vertexId: " + i + " has a null adj list! METIS may not work!!"
//					+ " numIncomingEdges: " + numIncomingEdges[i] + " numDegrees: " + numDegrees[i]);
//			}
			bufferedWriter.write("\n");
			tmpIntList.clear();
		}
		bufferedWriter.close();
	}

	public static void metisEdgeCounter(String inputFile) throws IOException {
		BufferedReader bufferedReader = getBufferedReader(inputFile);
		String line = null;
		int counter = 0;
		String[] split = null;
		long previousTime = System.currentTimeMillis();
		int numEdges = 0;
		// Ignore the first line.
		bufferedReader.readLine();
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			split = line.split("\\s+");
			if (counter % 1000000 == 0) {
				long currentTime = System.currentTimeMillis();
				System.out.println("Parsed " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			numEdges += (split.length - 3);
		}
		bufferedReader.close();
		System.out.println("numEdges: " + numEdges);
		System.out.println("numEdges/2: " + (numEdges/2));
	}

	public static void cleanWebUK(String inputFile, String outputFile) throws IOException {
		BufferedReader bufferedReader = getBufferedReader(inputFile);
		BufferedWriter bufferedWriter = getBufferedWriter(outputFile);
		String line = null;
		int counter = 0;
		long previousTime = System.currentTimeMillis();
		int numEdges = 0;
		int numVertices = 39459926;
		// Ignore the first line 39459926 lines.
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			if (counter % 10000000 == 0) {
				long currentTime = System.currentTimeMillis();
				System.out.println("Parsed " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			if (counter > numVertices) {
				bufferedWriter.write(line);
				bufferedWriter.write("\n");
			}
		}
		bufferedReader.close();
		bufferedWriter.close();
		System.out.println("numEdges: " + (counter - numVertices));
	}

	public static Graph[] getGraphs(String inputFile, int numNodes, boolean isUndirected,
		boolean bothDirectedAndUndirected) throws IOException {
		if (bothDirectedAndUndirected) {
			isUndirected = true;
		}
		int[] edgeCount = null;
		int[] edgeCountDirected = null;
		int[] edgeCountUndirected = null;
		if (!bothDirectedAndUndirected) {
			edgeCount = getEdgeCount(inputFile, numNodes, isUndirected);
		} else {
			edgeCountDirected = getEdgeCount(inputFile, numNodes, false /* directed */);
			edgeCountUndirected = getEdgeCount(inputFile, numNodes, true /* undirected */);
		}
		BufferedReader bufferedReader = getBufferedReader(inputFile);
		int[][] graph = null;
		int[][] graphUndirected = null;
		int[][] graphDirected = null;
		if (!bothDirectedAndUndirected) {
			graph = new int[numNodes + 1][];
		} else {
			graphUndirected = new int[numNodes + 1][];
			graphDirected = new int[numNodes + 1][];
		}
		for (int i = 0; i < numNodes + 1; ++i) {
			if (!bothDirectedAndUndirected) {
				graph[i] = new int[edgeCount[i]];
			} else {
				graphUndirected[i] = new int[edgeCountUndirected[i]];
				graphDirected[i] = new int[edgeCountDirected[i]];
			}
		}
		int[] numEdgesPerNode = null;
		int[] numEdgesPerNodeDirected = null;
		int[] numEdgesPerNodeUndirected = null;
		if (!bothDirectedAndUndirected) {
			numEdgesPerNode = new int[numNodes + 1];
		} else {
			numEdgesPerNodeDirected = new int[numNodes + 1];
			numEdgesPerNodeUndirected = new int[numNodes + 1];
		}
		int numEdges = 0;
		long previousTime = System.currentTimeMillis();
		System.out.println("starting...");
		int counter = 0;
		String line = null;
		String[] split = null;
		int source, destination;
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			split = line.split("\\s+");
			if (counter % 1000000 == 0) {
				long currentTime = System.currentTimeMillis();
				System.out.println("Parsed " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			try {
				source = Integer.parseInt(split[0]);
				destination = Integer.parseInt(split[1]);
				if (!bothDirectedAndUndirected) {
					graph[source][numEdgesPerNode[source]++] = destination;
					numEdges++;
					if (isUndirected) {
						graph[destination][numEdgesPerNode[destination]++] = source;
						numEdges++;
					}
				} else {
					numEdges += 2;
					graphDirected[source][numEdgesPerNodeDirected[source]++] = destination;
					graphUndirected[source][numEdgesPerNodeUndirected[source]++] = destination;
					graphUndirected[destination][numEdgesPerNodeUndirected[destination]++] = source;
				}
			} catch (NumberFormatException e) {
			}
		}
		if (!bothDirectedAndUndirected) {
			return new Graph[] { new Graph(numEdges, graph, isUndirected) };
		} else {
			return new Graph[] { new Graph(numEdges, graphUndirected, true /* is undirected */),
					new Graph(numEdges / 2, graphDirected, false /* is directed */) };
		}
	}

	public static BufferedReader getBufferedReader(String inputFile) throws FileNotFoundException {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile), 10000000);
		return bufferedReader;
	}

	public static int countNumNodes(String inputFile) throws IOException {
		BufferedReader bufferedReader = getBufferedReader(inputFile);
		String line = null;
		String[] split = null;
		int counter = 0;
		HashSet<Integer> nodeIds = new HashSet<Integer>();
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			split = line.split("\\s+");
			if (counter % 1000000 == 0) {
				System.out.println("Parsed " + counter + " lines in countNumNodes");
			}
			try {
				nodeIds.add(Integer.parseInt(split[0]));
				nodeIds.add(Integer.parseInt(split[1]));
			} catch (NumberFormatException e) {
			}
		}
		System.out.println("numNodes: " + nodeIds.size());
		return nodeIds.size();
	}

//	public static void convertSnapFileToMetisFormat(String inputFile,
//		boolean withVertexDegreeWeights, boolean withVertexSizeWeights, boolean withEdgeWeights,
//		String optOutputSuffix)
//		throws FileNotFoundException, IOException {
//		//		for (int i : graph.keySet()) {
////			System.out.println("originalGraph vertexId: " + i);
////		}
//		Map<Integer, Map<Integer, Integer>> weightedGraph =
//			readSnapFileIntoGraphAndMakeUndirectedAndWeighted(inputFile);
//		Map<Integer, Integer> vertexSizes = new HashMap<Integer, Integer>();
//		Map<Integer, Integer> numEdgesWithinCluster = new HashMap<Integer, Integer>();
//		for (int vertexId : weightedGraph.keySet()) {
//			vertexSizes.put(vertexId, 1);
//			numEdgesWithinCluster.put(vertexId, 0);
//		}
//		writeWeightedGraphInMETISFormat(inputFile, withVertexDegreeWeights, withVertexSizeWeights,
//			withEdgeWeights, vertexSizes, numEdgesWithinCluster, weightedGraph);
//	}

	public static void writeClusterIdOfEachVertex(
		Map<Integer, SPVertex<RandomCoarseningMessage>> weightedGraph,
		String outputFile) throws IOException {
		System.out.println("Writing cluster id of each vertex to: " + outputFile);
		BufferedWriter bufferedWriter = getBufferedWriter(outputFile);
		for (Entry<Integer, SPVertex<RandomCoarseningMessage>> entry
			: weightedGraph.entrySet()) {
			bufferedWriter.write(entry.getKey() + " " + entry.getValue().state.superNodeId + "\n");
		}
		bufferedWriter.close();
	}

	private static void writeRelabelsMap(Map<Integer, Integer> relabelsMap, String outputFile)
		throws IOException {
		System.out.println("Writing relabels map to: " + outputFile);
		BufferedWriter bufferedWriter = getBufferedWriter(outputFile);
		for (Entry<Integer, Integer> entry
			: relabelsMap.entrySet()) {
			bufferedWriter.write(entry.getKey() + " " + entry.getValue() + "\n");
		}
		bufferedWriter.close();
	}

	public static void writeWeightedGraphInMETISFormat(String outputFile,
		boolean withVertexDegreeWeights, boolean withVertexSizeWeights, boolean withEdgeWeights,
		Map<Integer, SPVertex<RandomCoarseningMessage>> weightedGraph,
		boolean writeRelabelsMapAndStates) throws IOException {
		if (writeRelabelsMapAndStates) {
			writeClusterIdOfEachVertex(weightedGraph, outputFile + DOT_ORIGINALCLUSTERIDS);
		}
		Map<Integer, SPVertex<RandomCoarseningMessage>> coarsenedGraph = new HashMap<Integer, SPVertex<RandomCoarseningMessage>>();
		for (Entry<Integer, SPVertex<RandomCoarseningMessage>> entry : weightedGraph.entrySet()) {
			if (entry.getValue().state.skip) {
				continue;
			}
			coarsenedGraph.put(entry.getKey(), entry.getValue());
		}
		makeWeightedGraphUndirected(coarsenedGraph);
		Map<Integer, Integer> relabelsMap = getRelabelsMap(coarsenedGraph);
		if (writeRelabelsMapAndStates) {
			writeRelabelsMap(relabelsMap, outputFile + DOT_RELABELSMAP);
		}
		Map<Integer, SPVertex<RandomCoarseningMessage>> relabeledGraph = relabelVerticesFromOneToN(
			coarsenedGraph, relabelsMap);
		List<Integer> sortedVertices = new ArrayList<Integer>(relabeledGraph.keySet());
		Collections.sort(sortedVertices);
		int numEdges = countNumEdges(relabeledGraph, false);
		int numEdgesWithWeights = countNumEdges(relabeledGraph, true);
		Pair<Integer, Integer> pairOfsNumVerticesAndNumEdgesWithSupernodes =
			countTotalNumVerticesAndEdgesWithinSupernodes(relabeledGraph);
		int numVertices = relabeledGraph.size();
		int numTotalVertices = pairOfsNumVerticesAndNumEdgesWithSupernodes.fst;
		int totalNumEdgesWithinCluster = pairOfsNumVerticesAndNumEdgesWithSupernodes.snd;
		System.out.println("numEdges: " + numEdges);
		System.out.println("numEdgesWithDegrees: " + numEdgesWithWeights);
		System.out.println("totalNumEdgesWithinCluster: " + totalNumEdgesWithinCluster);
		System.out.println("numVertices: " + numVertices);
		System.out.println("numTotalVertices: " + numTotalVertices);
		if (!((numEdges % 2) == 0)) {
			System.out.println("numEdges is not an even number in an undirected graph. "
				+ numEdges);
		}
//		String outputFileSuffix = withVertexDegreeWeights ? ".vertexDegreeWeights" : "";
//		outputFileSuffix += withVertexSizeWeights ? ".vertexSizeWeights" : "";
//		String outputFile = inputFile + outputFileSuffix + optOutputSuffix + ".mgraph";
		System.out.println("Writing file: " + outputFile + " in METIS format");
		BufferedWriter bufferedWriter = getBufferedWriter(outputFile);
		String fmt = "";
		if (withEdgeWeights && withVertexDegreeWeights) {
			fmt = " 011";
		} else if (withVertexDegreeWeights) {
			fmt = " 010";
		}
		fmt += withVertexSizeWeights ? " 2" : "";
		System.out.println("Writing the first line:");
		bufferedWriter.write(numVertices + " "
			+ (numEdges/2) + fmt + "\n");
		System.out.println("Wrote the first line:");
		int totalDegrees = 0;
		Map<Integer, Integer> neighborIds;
		int sumEdgesWithinSuperNode;
		String neighborsPrintStr;
		SPVertex spVertex;
		for (int i = 0; i < sortedVertices.size(); ++i) {
			int relabeledVertexId = i+1;
			if ((relabeledVertexId % 100000) == 0) {
				System.out.println("Writing " + relabeledVertexId + "th line.");
			}
			if (!relabeledGraph.containsKey(relabeledVertexId)) {
				System.out.println("Graph should always contain a continuous vertices from 1 to " +
					"n after relabeling.");
			}
			spVertex = relabeledGraph.get(relabeledVertexId);
			if (spVertex.state.skip) {
				continue;
			}
			neighborIds = spVertex.weightedNeighbors;
			sumEdgesWithinSuperNode = spVertex.numEdgesWithinVertex; // + getDegree(neighborIds);
			totalDegrees += sumEdgesWithinSuperNode;
			neighborsPrintStr = withVertexDegreeWeights ? " " + sumEdgesWithinSuperNode : "";
			neighborsPrintStr += withVertexSizeWeights ? " " + spVertex.numVerticesWithinVertex : "";
			bufferedWriter.write(neighborsPrintStr);
			for (int neighborId : neighborIds.keySet()) {
				bufferedWriter.write(" ");
				bufferedWriter.write("" + neighborId);
//				neighborsPrintStr += " " + neighborId;
				if (withEdgeWeights) {
					bufferedWriter.write(" ");
					bufferedWriter.write("" + neighborIds.get(neighborId));
//					neighborsPrintStr += " " + neighborIds.get(neighborId);
				}
			}
			bufferedWriter.write("\n");
//			bufferedWriter.write(neighborsPrintStr + "\n");
		}
		System.out.println("totalDegrees: " + totalDegrees);
		bufferedWriter.close();
	}

	private static void makeWeightedGraphUndirected(
		Map<Integer, SPVertex<RandomCoarseningMessage>> weightedGraph) {
		List<Integer> vertexIds = new ArrayList<Integer>(weightedGraph.keySet());
		Map<Integer, Integer> weightedNeighbors;
		Map<Integer, Integer> neighborsWeightedNeighbors;
		int currentDegree;
		int neighborsDegree;
		int averageDegree;
		for (int vertexId : vertexIds) {
			weightedNeighbors = weightedGraph.get(vertexId).weightedNeighbors;
			for (Entry<Integer, Integer> weightedNeighbor : weightedNeighbors.entrySet()) {
				neighborsWeightedNeighbors = weightedGraph.get(weightedNeighbor.getKey()).weightedNeighbors;
				currentDegree = weightedNeighbor.getValue();
				if (currentDegree <= 0) {
					System.out.println("CURRENT DEGREE CANNOT BE 0!!!. Exiting");
					System.exit(-1);
				}
				neighborsDegree = neighborsWeightedNeighbors.containsKey(vertexId) ?
					neighborsWeightedNeighbors.get(vertexId) : 0;
				averageDegree = Math.max(1, (currentDegree + neighborsDegree) / 2);
				weightedNeighbor.setValue(averageDegree);
				neighborsWeightedNeighbors.put(vertexId, averageDegree);
			}
		}
	}

	private static Pair<Integer, Integer> countTotalNumVerticesAndEdgesWithinSupernodes(
		Map<Integer, SPVertex<RandomCoarseningMessage>> relabeledGraph) {
		int numVerticesWithinSupernodes = 0;
		int numEdgesWithinSupernodes = 0;
		for (SPVertex<RandomCoarseningMessage> spVertex : relabeledGraph.values()) {
			numEdgesWithinSupernodes += spVertex.numEdgesWithinVertex;
			numVerticesWithinSupernodes += spVertex.numVerticesWithinVertex;
		}
		return Pair.of(numVerticesWithinSupernodes, numEdgesWithinSupernodes);
	}

	static int getDegree(Map<Integer, Integer> neighborIds) {
		int sum = 0;
		for (int degree : neighborIds.values()) {
			sum += degree;
		}
		return sum;
	}

	public static int countNumEdges(Map<Integer, SPVertex<RandomCoarseningMessage>> weightedGraph, boolean withWeights) {
		int numEdges = 0;
		for (Entry<Integer, SPVertex<RandomCoarseningMessage>> entry : weightedGraph.entrySet()) {
			numEdges += withWeights ? countWeightedEdges(entry.getValue())
				: entry.getValue().getWeightedNeighbors().size();
		}
		return numEdges;
	}

	public static int countWeightedEdges(SPVertex spVertex) {
		int sum = 0;
		for (int value : ((Map<Integer, Integer>) spVertex.getWeightedNeighbors()).values()) {
			sum += value;
		}
		return sum;
	}

	public static Map<Integer, SPVertex<RandomCoarseningMessage>> relabelVerticesFromOneToN(
		Map<Integer, SPVertex<RandomCoarseningMessage>> graph, Map<Integer, Integer> relabelsMap) {
		System.out.println("original graph.size(): " + graph.size());
		Map<Integer, SPVertex<RandomCoarseningMessage>> relabeledGraph = new HashMap<Integer, SPVertex<RandomCoarseningMessage>>();
		SPVertex spVertex;
		Map<Integer, Integer> originalNeighbors;
		for (int vertexId : graph.keySet()) {
			HashMap<Integer, Integer> relabeledNeighbors = new HashMap<Integer, Integer>();
			Integer relabeledVertexId = relabelsMap.get(vertexId);
			spVertex = graph.get(vertexId);
			originalNeighbors = (Map<Integer, Integer>) spVertex.weightedNeighbors;
			//			relabeledVertexSizes.put(relabeledVertexId, vertexSizes.get(vertexId));
//			relabeledNumEdgesInCluster.put(relabeledVertexId, numEdgesWithinCluster.get(vertexId));
			spVertex.weightedNeighbors = relabeledNeighbors;
			relabeledGraph.put(relabeledVertexId, spVertex);
			for (Entry<Integer, Integer> entry : originalNeighbors.entrySet()) {
				Integer relabeledNeighborId = relabelsMap.get(entry.getKey());
				if (relabeledNeighborId == null) {
					System.out.println("relabeled neighborId is null. vertexId: " + vertexId + " neighborId: " + entry.getKey());
				}
				relabeledNeighbors.put(relabeledNeighborId, entry.getValue());
			}
		}
		return relabeledGraph;
	}

	public static Map<Integer, Integer> getRelabelsMap(Map<Integer, SPVertex<RandomCoarseningMessage>> graph) {
		Map<Integer, Integer> relabelsMap = new HashMap<Integer, Integer>();
//		List<Integer> sortedVertices = new ArrayList<Integer>(graph.keySet());
//		Collections.sort(sortedVertices);
		int counter = 1;
		for (Entry<Integer, SPVertex<RandomCoarseningMessage>> entry : graph.entrySet()) {
			if (entry.getValue().state.skip) {
				continue;
			}
//			System.out.println("relabeling " + actualId + " to: " + i);
			relabelsMap.put(entry.getKey(), counter++);
		}
		return relabelsMap;
	}

	public static Map<Integer, HashSet<Integer>> readGPSFileIntoGraphAndMakeUndirected(
		String inputFile) throws FileNotFoundException, IOException {
		BufferedReader bufferedReader = PartitionerUtils.getBufferedReader(inputFile);
		String line;
		int counter = 0;
		String[] split;
		Map<Integer, HashSet<Integer>> graph = new HashMap<Integer, HashSet<Integer>>();
		int source;
		int destination;
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			split = line.split("\\s+");
			if (counter % 1000000 == 0) {
				System.out.println("Parsed " + counter + "th line.");
			}
			try {
				source = Integer.parseInt(split[0]); // + 1;
				for (int i = 1; i < split.length; ++i) {
					destination = Integer.parseInt(split[i]); // + 1;
					if (!graph.containsKey(source)) {
						graph.put(source, new HashSet<Integer>());
					}
					if (!graph.containsKey(destination)) {
						graph.put(destination, new HashSet<Integer>());
					}
					// TODO(semih): Make this a flag. We're currently cleaning
					// self-loops
					if (source != destination) {
						graph.get(source).add(destination);
						graph.get(destination).add(source);
					}
				}
			} catch (Exception e) {
				System.out.println("Something is wrong with this line: " + line + " skipping...");
				// Skipping this line;
			}
		}
		System.out.println("Finished reading the graph from GPSFormat.");
		bufferedReader.close();
		return graph;
	}
	
	public static Map<Integer, Map<Integer, Integer>> readSnapFileIntoGraphAndMakeUndirectedAndWeighted(
		String inputFile) throws FileNotFoundException, IOException {
		BufferedReader bufferedReader = PartitionerUtils.getBufferedReader(inputFile);
		String line;
		int counter = 0;
		String[] split;
		Map<Integer, Map<Integer, Integer>> graph = new HashMap<Integer, Map<Integer, Integer>>();
		int source;
		int destination;
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			split = line.split("\\s+");
			if (counter % 1000000 == 0) {
				System.out.println("Parsed " + counter + "th line.");
			}
			try {
				source = Integer.parseInt(split[0]); // + 1;
				destination = Integer.parseInt(split[1]); // + 1;
				if (!graph.containsKey(source)) {
					graph.put(source, new HashMap<Integer, Integer>());
				}
				if (!graph.containsKey(destination)) {
					graph.put(destination, new HashMap<Integer, Integer>());
				}
				// TODO(semih): Make this a flag. We're currently cleaning self-loops
				if (source != destination) {
					graph.get(source).put(destination, 1);
					graph.get(destination).put(source, 1);
				}
			} catch (Exception e) {
				System.out.println("Something is wrong with this line: " + line + " skipping...");
				// Skipping this line;
			}
		}
		System.out.println("Finished reading the graph from SnapFormat.");
		bufferedReader.close();
		return graph;
	}

	public static Map<Integer, HashSet<Integer>> readSnapFileIntoGraphAndMakeUndirected(
		String inputFile) throws FileNotFoundException, IOException {
		Map<Integer, Map<Integer, Integer>> undirectedWeightedGraph =
			readSnapFileIntoGraphAndMakeUndirectedAndWeighted(inputFile);

		Map<Integer, HashSet<Integer>> graph = new HashMap<Integer, HashSet<Integer>>();
		for (Entry<Integer, Map<Integer, Integer>> entry : undirectedWeightedGraph.entrySet()) {
			HashSet<Integer> neighborIds = new HashSet<Integer>();
			graph.put(entry.getKey(), neighborIds);
			neighborIds.addAll(entry.getValue().keySet());
		}
		return graph;
	}
	
	public static void dumpPartitionStatistics(String inputFile, int numPartitions,
		boolean withVertexDegreeWeights, boolean withVertexSizeWeights, boolean withEdgeWeights) throws IOException {
		Pair<Map<Integer, Map<Integer, Integer>>, Pair<Map<Integer, Integer>, Map<Integer, Integer>>>
			readMETISFileIntoGraph =
			readMETISFileIntoGraph(inputFile, withVertexDegreeWeights,
				withVertexSizeWeights, withEdgeWeights);
		Map<Integer, Map<Integer, Integer>> weightedGraph = readMETISFileIntoGraph.fst;
		Map<Integer, Integer> vertexWeights = readMETISFileIntoGraph.snd.fst;
		Map<Integer, Integer> numEdgesWithinCluster = readMETISFileIntoGraph.snd.snd;
		List<Integer> partitionIds = readMETISPartitions(inputFile + ".part." + numPartitions);
		int[] numVerticesInPartitions = new int[numPartitions];
		int[] numEdgesInPartitions = new int[numPartitions];
		int numEdgesCrossingPartitions = 0;
		int totalNumEdges = 0;
		int vertexPartitionId;

		for (Entry<Integer, Map<Integer, Integer>> entry : weightedGraph.entrySet()) {
			int vertexId = entry.getKey();
			Map<Integer, Integer> weightedNeighbors = entry.getValue();
			vertexPartitionId = partitionIds.get(vertexId-1);
			numVerticesInPartitions[vertexPartitionId] += vertexWeights.get(vertexId);
//			int numWeightedEdges = countWeightedEdges(weightedNeighbors);
			numEdgesInPartitions[vertexPartitionId] += numEdgesWithinCluster.get(vertexId);
			totalNumEdges += numEdgesWithinCluster.get(vertexId); // + numWeightedEdges
			if ((vertexId % 300) == 0) {
				System.out.println("Counting " + vertexId + "th verex. NumTotalEdges: "
					+ totalNumEdges + " numEdgesCrossingPartitions: " + numEdgesCrossingPartitions);
			}
			for (Entry<Integer, Integer> edgeEntry : weightedNeighbors.entrySet()) {
				if (vertexPartitionId != partitionIds.get(edgeEntry.getKey()-1)) {
					numEdgesCrossingPartitions += edgeEntry.getValue();
				}
			}
		}
		
		for (int i = 0; i < numPartitions; ++i) {
			System.out.println("partitionNo: " + i + " numVertices: "
				+ numVerticesInPartitions[i] + " numEdges: " + numEdgesInPartitions[i]);
		}
		System.out.println("totalNumEdges: " + totalNumEdges);
		System.out.println("numEdgesCrossingPartitions: " + numEdgesCrossingPartitions);
	}

	static List<Integer> readMETISPartitions(String partFile)
		throws NumberFormatException, IOException {
		BufferedReader bufferedReader = PartitionerUtils.getBufferedReader(partFile);
		String line;
		List<Integer> partitions = new ArrayList<Integer>();
		while ((line = bufferedReader.readLine()) != null) {
			int partitionId = Integer.parseInt(line);
//			System.out.println("vertexId: " + counter + " has partitionId: " + partitionId);
			partitions.add(partitionId);
		}
		bufferedReader.close();
		System.out.println("Read metis partitionsFile: " + partFile);
		return partitions;
	}

	public static void countNumEdgesCrossingAfterMETIS(String originalGraphFile,
		String metisPartsFile) throws IOException {
		Map<Integer, Integer> partitionsMap = getPartitionsMap(metisPartsFile);
		BufferedReader bufferedReader = getBufferedReader(originalGraphFile);
		String line = null;
		int partitionIdOfVertexId;
		int counter = 0;
		String[] split;
		long previousTime = System.currentTimeMillis();
		int numEdgesCrossingPartitions = 0;
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			if (counter % 1000000 == 0) {
				long currentTime = System.currentTimeMillis();
				System.out.println("Parsed METIS partitions file " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			split = line.split("\\s+");
			try {
				partitionIdOfVertexId = partitionsMap.get(Integer.parseInt(split[0]));
				for (int i = 1; i < split.length; ++i) {
					if (partitionIdOfVertexId != partitionsMap.get(Integer.parseInt(split[i]))) {
						numEdgesCrossingPartitions++;
					}
				}
			} catch (NumberFormatException e) {
				// Do nothing
			}
		}
		bufferedReader.close();
		System.out.println("numEdgesCrossingPartitions: " + numEdgesCrossingPartitions);
	}
	
	private static Map<Integer, Integer> getPartitionsMap(String metisPartsFile) throws IOException {
		BufferedReader bufferedReader = getBufferedReader(metisPartsFile);
		String line = null;
		int counter = 0;
		Map<Integer, Integer> partitionsMap = new HashMap<Integer, Integer>();
		long previousTime = System.currentTimeMillis();
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			if (counter % 1000000 == 0) {
				long currentTime = System.currentTimeMillis();
				System.out.println("Parsed METIS partitions file " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			partitionsMap.put(counter - 1, Integer.parseInt(line));
		}
		return partitionsMap;
	}

	public static void countNumEdgesCrossingAfterCoarseningAndMETIS(String originalFileName,
		String metisFile, int numParts) throws FileNotFoundException, IOException {
		Map<Integer, Map<Integer, Integer>> regularGraph =
			PartitionerUtils.readSnapFileIntoGraphAndMakeUndirectedAndWeighted(originalFileName);
		Map<Integer, SPVertex<RandomCoarseningMessage>> weightedGraph =
			convertRegularGraphIntoSequentialPregelRandomCoarseningGraph(regularGraph);
		List<Integer> partitions = readMETISPartitions(metisFile + ".part." + numParts);
		Map<Integer, Integer> reverseRelabelsMap = getMap(metisFile + DOT_RELABELSMAP,
			false /* don't reverse */);
		Map<Integer, Integer> originalClusterIdsMap = getMap(metisFile + DOT_ORIGINALCLUSTERIDS,
			false /* don't reverse */);
		
		for (Entry<Integer, SPVertex<RandomCoarseningMessage>> entry : weightedGraph.entrySet()) {
			entry.getValue().state.superNodeId = originalClusterIdsMap.get(entry.getKey());
		}
		
		// Set the METIS partitions of supernodes.
		SPVertex<RandomCoarseningMessage> spVertex;
		for (int superNodeId : reverseRelabelsMap.keySet()) {
			spVertex = weightedGraph.get(superNodeId);
			spVertex.state.superNodeId = partitions.get(reverseRelabelsMap.get(superNodeId) - 1);
			spVertex.state.skip = true;
		}
		
		// Set the partitions of subnodes.
		State state;
		for (Entry<Integer, SPVertex<RandomCoarseningMessage>> entry : weightedGraph.entrySet()) {
			spVertex = entry.getValue();
			state = spVertex.state;
			if (spVertex.state.skip) {
				continue;
			} else {
				state.superNodeId = partitions.get(reverseRelabelsMap.get(state.superNodeId) - 1);
			}
		}

		int[] numVerticesPerPartition = new int[numParts];
		int[] numEdgesPerPartition = new int[numParts];
		int numEdgesWithinPartitions = 0;
		int numEdgesCrossingPartitions = 0;
		// Count statistics
		List<Integer> idList = new ArrayList<Integer>(weightedGraph.keySet());
		SPVertex<RandomCoarseningMessage> neighborSpVertex;
		for (int vertexId : idList) {
			spVertex = weightedGraph.get(vertexId);
			state = spVertex.state;
			numVerticesPerPartition[state.superNodeId]++;
			for (Entry<Integer, Integer> weightedNeighbor : spVertex.weightedNeighbors.entrySet()) {
				neighborSpVertex = weightedGraph.get(weightedNeighbor.getKey());
				if (neighborSpVertex.state.superNodeId == state.superNodeId) {
					numEdgesWithinPartitions += weightedNeighbor.getValue();
				} else {
					numEdgesCrossingPartitions += weightedNeighbor.getValue();
				}
				numEdgesPerPartition[state.superNodeId] += weightedNeighbor.getValue();
			}
		}

		for (int i = 0; i < numParts; ++i) {
			System.out.println("Partition " + i + " numVertices: " + numVerticesPerPartition[i]
			     + "\tnumEdges: " + numEdgesPerPartition[i]);
		}
		System.out.println("totalNumEdges: " + (numEdgesWithinPartitions + numEdgesCrossingPartitions));
		System.out.println("totalNumEdgesWithinPartitions: " + numEdgesWithinPartitions);
		System.out.println("totalNumEdgesCrossingPartitions: " + numEdgesCrossingPartitions);
	}

	private static Map<Integer, Integer> getMap(String mapFile, boolean reverse) throws NumberFormatException, IOException {
		BufferedReader bufferedReader = PartitionerUtils.getBufferedReader(mapFile);
		String line;
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		String[] split;
		while ((line = bufferedReader.readLine()) != null) {
			split = line.split("\\s+");
			if (reverse) {
				map.put(Integer.parseInt(split[1]), Integer.parseInt(split[0]));
			} else {				
				map.put(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
			}
		}
		bufferedReader.close();
		System.out.println("Read mapFile: " + mapFile);
		return map;
	}

	public static Map<Integer, SPVertex<RandomCoarseningMessage>> convertRegularGraphIntoSequentialPregelRandomCoarseningGraph(
		Map<Integer, Map<Integer, Integer>> regularGraph) {
		Map<Integer, SPVertex<RandomCoarseningMessage>> randomCoarseningGraph =
			new HashMap<Integer, SPVertex<RandomCoarseningMessage>>();
		for (Entry<Integer, Map<Integer, Integer>> entry : regularGraph.entrySet()) {
			randomCoarseningGraph.put(entry.getKey(),
				new SPVertex<RandomCoarseningMessage>(entry.getKey(), entry.getValue()));
		}
		return randomCoarseningGraph;
	}

	public static Pair<Map<Integer, Map<Integer, Integer>>,
	Pair<Map<Integer, Integer>, Map<Integer, Integer>>> readMETISFileIntoGraph(String inputFile,
		boolean withVertexDegreeWeights, boolean withVertexSizeWeights, boolean withEdgeWeights)
		throws IOException {
		Map<Integer, Map<Integer, Integer>> weightedGraph = new HashMap<Integer, Map<Integer,Integer>>();
		Map<Integer, Integer> vertexWeights = new HashMap<Integer, Integer>();
		Map<Integer, Integer> numEdgesWithinClusters = new HashMap<Integer, Integer>();
		BufferedReader bufferedReader = PartitionerUtils.getBufferedReader(inputFile);
		String line;
		int counter = 0;
		String[] split;
		// Skip first line.
		line = bufferedReader.readLine();
		int numVertices = 0;
		int numEdges = 0;
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			numVertices++;
			line = line.trim();
			split = line.split("\\s+");
			Map<Integer, Integer> weightedNeighbors = new HashMap<Integer, Integer>();
//			List<Integer> neighborsSet = new ArrayList<Integer>();
			weightedGraph.put(counter, weightedNeighbors);
			if (counter % 1000000 == 0) {
				System.out.println("Parsed " + counter + "th line.");
			}
			try {
				int startIndex = 0;
				startIndex = withVertexDegreeWeights ? startIndex + 1 : startIndex;
				startIndex = withVertexSizeWeights ? startIndex + 1 : startIndex;
				if (withVertexDegreeWeights) {
					numEdgesWithinClusters.put(counter, Integer.parseInt(split[0]));
				} else {
					numEdgesWithinClusters.put(counter, 0);
				}
				if (!withVertexSizeWeights) {
					vertexWeights.put(counter, 1);
				} else {
					vertexWeights.put(counter, Integer.parseInt(split[startIndex - 1]));
				}
				for (int i = startIndex; i < split.length; ++i) {
					numEdges++;
					if (!withEdgeWeights) {
						weightedNeighbors.put(Integer.parseInt(split[i]), 1);
					} else {
						weightedNeighbors.put(Integer.parseInt(split[i]), Integer.parseInt(split[i+1]));
						i++;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Something is wrong with this line: " + line + " skipping... lineNo: " + counter);
				// Skipping this line;
			}
		}
		System.out.println("Finished reading the graph from METIS Format. numVertices: "
			+ numVertices + " numEdges: " + numEdges);
		bufferedReader.close();
		return Pair.of(weightedGraph, Pair.of(vertexWeights, numEdgesWithinClusters));
	}

	public static Map<Integer, Map<Integer, Integer>> convertToWeightedEdges(
		Map<Integer, HashSet<Integer>> graph) {
		Map<Integer, Map<Integer, Integer>> weightedGraph =
			new HashMap<Integer, Map<Integer, Integer>>();
		for (int vertexId : graph.keySet()) {
			Map<Integer, Integer> weightedNeighbors = new HashMap<Integer, Integer>();
			weightedGraph.put(vertexId, weightedNeighbors);
			for (int neighborId : graph.get(vertexId)) {
				weightedNeighbors.put(neighborId, 1);
			}
		}
		return weightedGraph;
	}

	public static int[][] readGPSGraphIntoArray(FileSystem fileSystem, String inputFilePrefix, int numVertices) throws NumberFormatException, IOException {
		int[][] graph = new int[numVertices][];
		for (int i = 0; i < graph.length; ++i) {
			graph[i] = new int[1];
		}
		int[] neighborIdIndices = new int[numVertices];
		String[] split;
		int source;
		int destination;
		String line;
		int counter = 0;
		FileStatus[] fileStatusArray = fileSystem.globStatus(new Path(inputFilePrefix));
		for (FileStatus fileStatus : fileStatusArray) {
			String filePath = fileStatus.getPath().toUri().getPath();
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
				fileSystem.open(new Path(filePath))));
			int neighborIdIndex = -1;
			int[] adjacencyList = null;
			while ((line = bufferedReader.readLine()) != null) {
				counter++;
				split = line.split("\\s+");
				if (counter % 1000000 == 0) {
					System.out.println("Parsed " + counter + "th line.");
				}
				source = Integer.parseInt(split[0]);
				adjacencyList = graph[source];
				neighborIdIndex = neighborIdIndices[source];
				for (int i = 1; i < split.length; ++i) {
					destination = Integer.parseInt(split[i]); // + 1;
					if (neighborIdIndex == adjacencyList.length) {
						resizeAdjacencyListOfVertex(graph, source);
						adjacencyList = graph[source];
					}
					graph[source][neighborIdIndex] = destination;
					neighborIdIndex++;
					neighborIdIndices[source] = neighborIdIndex;
				}
			}
			System.out.println("Finished reading GPS graph file: " + filePath + " into array.");
			bufferedReader.close();
		}
		for (int i = 0; i < graph.length; ++i) {
			shrinkAdjacencyListIfNecessary(graph, neighborIdIndices, i);
		}
		return graph;
	}

	private static void shrinkAdjacencyListIfNecessary(int[][] graph, int[] neighborIdIndices, int source) {
		int[] oldAdjacencyList = graph[source];
		int neighborIdIndex = neighborIdIndices[source];
		if (oldAdjacencyList.length > neighborIdIndex) {
			int[] newAdjacencyList = new int[neighborIdIndex];
			System.arraycopy(oldAdjacencyList, 0, newAdjacencyList, 0, neighborIdIndex);
			graph[source] = newAdjacencyList;
		}
	}

	private static void resizeAdjacencyListOfVertex(int[][] graph, int source) {
		int[] oldAdjacencyList = graph[source];
		int[] newAdjacencyList = new int[oldAdjacencyList.length * 2];
		System.arraycopy(oldAdjacencyList, 0, newAdjacencyList, 0, oldAdjacencyList.length);
		graph[source] = newAdjacencyList;
	}
}
