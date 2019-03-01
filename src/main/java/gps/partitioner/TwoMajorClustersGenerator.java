package gps.partitioner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class TwoMajorClustersGenerator {

	public static void main(String[] args) throws IOException {
		int numPerCluster = 50000;
		int degree = 30;
		Map<Integer, HashSet<Integer>> graph1 = getGraph(numPerCluster);
		populateAndCorrectGraph(degree, graph1);
		Map<Integer, HashSet<Integer>> graph2 = getGraph(numPerCluster);
		populateAndCorrectGraph(degree, graph2);
		Map<Integer, HashSet<Integer>> mergedGraph = getGraph(numPerCluster*2);

		for (int vertexId : graph1.keySet()) {
			mergedGraph.put(vertexId, graph1.get(vertexId));
		}
		for (int vertexId : graph2.keySet()) {
			for (int neighborId : graph2.get(vertexId)) {
				mergedGraph.get(vertexId + numPerCluster).add(neighborId + numPerCluster);
			}
		}
		mergedGraph.get(numPerCluster).add(numPerCluster+1);
		mergedGraph.get(numPerCluster+1).add(numPerCluster);
		SNAPToMatlabSparseMatrixConverter.outputMatlabFormat(
			"/Users/semihsalihoglu/data/two_major_clusters_"
			+ (numPerCluster*2) + ".mtl", mergedGraph);
	}

	private static void populateAndCorrectGraph(int degree,
		Map<Integer, HashSet<Integer>> graph) {
		Random random = new Random();
		int neighborId;
		int numPerCluster = graph.size();
		for (int sourceId = 1; sourceId <= numPerCluster; ++sourceId) {
			for (int i = 0; i < degree; ++i) {
				neighborId = sourceId <= numPerCluster ? random.nextInt(numPerCluster) + 1
					: random.nextInt(numPerCluster) + numPerCluster + 1;
				graph.get(sourceId).add(neighborId);
				graph.get(neighborId).add(sourceId);
				if ((sourceId <= numPerCluster && neighborId > numPerCluster) ||
					(sourceId > numPerCluster && neighborId <= numPerCluster)) {
					System.out.println("Wrong edge. Edge crossing two major clusters: sourceId: "
						+ sourceId + " neighborId: " + neighborId);
				}
			}
		}

		ConnectedComponents.checkAndCompleteConnectedComponent(graph);
	}

	private static Map<Integer, HashSet<Integer>> getGraph(int size) {
		Map<Integer, HashSet<Integer>> graph = new HashMap<Integer, HashSet<Integer>>();
		for (int i = 1; i <= size; ++i) {
			graph.put(i, new HashSet<Integer>());
		}
		return graph;
	}
}
