package gps.partitioner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ConnectedComponents {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		Map<Integer, HashSet<Integer>> graph =
			PartitionerUtils.readSnapFileIntoGraphAndMakeUndirected(args[0]);
		checkAndCompleteConnectedComponent(graph);
		printExceptionVertices(graph);
		String outputFile = args[0].substring(0, args[0].indexOf(".txt")) + ".mtl";
		SNAPToMatlabSparseMatrixConverter.outputMatlabFormat(outputFile, graph);
	}

	private static void printExceptionVertices(Map<Integer, HashSet<Integer>> graph) {
		printVertexAndNeighbors(graph, 1);
//		printVertexAndNeighbors(graph, 17137);
//		printVertexAndNeighbors(graph, 60508);
//		printVertexAndNeighbors(graph, 60509);
//		printVertexAndNeighbors(graph, 60510);
//		printVertexAndNeighbors(graph, 71747);
//		printVertexAndNeighbors(graph, 71750);
	}

	private static void printVertexAndNeighbors(Map<Integer, HashSet<Integer>> graph, int vertexId) {
		System.out.println("Starting printing the neighbors of: " + vertexId + " numNeighbors: "
			+ graph.get(vertexId).size());
//		for (int neighborId : graph.get(vertexId)) {
//			System.out.println(" " + neighborId);
//		}
		System.out.println("End of printing the neighbors of: " + vertexId);
	}

	public static void checkAndCompleteConnectedComponent(Map<Integer, HashSet<Integer>> graph) {
		System.out.println("graphSize: " + graph.size());
		for (int i = 0; i < 5; ++i) {
			System.out.println("Checking and correcting for " + (i+1) + "th time..");
			checkAndCorrectCC(graph);
		}
	}

	private static void checkAndCorrectCC(Map<Integer, HashSet<Integer>> graph) {
		checkIfThereAreMissingVertices(graph);
		int startId = graph.keySet().iterator().next();
		System.out.println("startId: " + startId);
		Set<Integer> visitedVertices = new HashSet<Integer>();
		visitedVertices.add(startId);
		Set<Integer> expandedVertices = new HashSet<Integer>();
		Set<Integer> unExpandedVertices = new HashSet<Integer>();
		unExpandedVertices.add(startId);
		while (!unExpandedVertices.isEmpty()) {
			int nextId = unExpandedVertices.iterator().next();
			unExpandedVertices.remove(nextId);
			expandedVertices.add(nextId);
			for (int neighborId : graph.get(nextId)) {
				visitedVertices.add(neighborId);
				if (!expandedVertices.contains(neighborId)) {
					unExpandedVertices.add(neighborId);
				}
			}
		}
		System.out.println("numVerticesVisited: " + visitedVertices.size());

		Set<Integer> unvisitedVertices = new HashSet<Integer>();
		for (int nodeId : graph.keySet()) {
			if (!visitedVertices.contains(nodeId)) {
				unvisitedVertices.add(nodeId);
			}
		}
		
		for (int unvisitedNodeId : unvisitedVertices) {
			Set<Integer> neighbors = graph.get(unvisitedNodeId);
			System.out.println("nodeId: " + unvisitedNodeId + " was not visited. numNeighbors: "
				+ neighbors.size());
			for (int neighborId : neighbors) {
				System.out.println("neighborId: " + neighborId);
			}
			neighbors.add(startId);
			graph.get(startId).add(unvisitedNodeId);
		}
	}

	private static void checkIfThereAreMissingVertices(Map<Integer, HashSet<Integer>> graph) {
		int maxId = findMaxId(graph.keySet());
		System.out.println("maxId: " + maxId);
		for (int i = 1; i <= maxId; ++i) {
			if (!graph.containsKey(i)) {
				graph.put(i, new HashSet<Integer>());
			}
		}
	}

	private static int findMaxId(Set<Integer> keySet) {
		int maxId = keySet.iterator().next();
		for (int key : keySet) {
			if (key > maxId) {
				maxId = key;
			}
		}
		return maxId;
	}
}
