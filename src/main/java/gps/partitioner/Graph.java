package gps.partitioner;

public class Graph {

	// For an undirected graph, the numEdges is 2xactual numberOfEdges.
	// Users of this class should know to divide it by two if they need.
	public int numEdges;
	public int[][] graph;
	public int[] originalIds = null;
	public boolean isUndirected = false;

	public Graph(int numEdges, int[][] graph, boolean isUndirected) {
		this.numEdges = numEdges;
		this.graph = graph;
		this.isUndirected = isUndirected;
	}

	// Only if this graph is a subgraph of another graph
	public Graph(int numEdges, int[][] graph, boolean isUndirected, int[] originalIds) {
		this(numEdges, graph, isUndirected);
		this.originalIds = originalIds;
	}
}
