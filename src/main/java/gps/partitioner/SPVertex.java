package gps.partitioner;

import java.util.Map;

import gps.partitioner.SequentialPregel.Message;
import gps.partitioner.SequentialPregel.State;

public class SPVertex<M extends Message> {
	State state;
	int numEdgesWithinVertex;
	int numVerticesWithinVertex;
	public Map<Integer, Integer> weightedNeighbors;

	public SPVertex(int vertexId, Map<Integer, Integer> weightedNeighbors) {
		this.weightedNeighbors = weightedNeighbors;
		this.numEdgesWithinVertex = PartitionerUtils.getDegree(weightedNeighbors);
		this.numVerticesWithinVertex = 1; // by default on the vertex itself is in it
		this.state = new State(vertexId);
	}
	
	public Map<Integer, Integer> getWeightedNeighbors() {
		return weightedNeighbors;
	}
}
