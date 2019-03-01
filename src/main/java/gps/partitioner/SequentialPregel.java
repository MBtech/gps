package gps.partitioner;

import gps.node.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public abstract class SequentialPregel<M extends SequentialPregel.Message> {
	
	protected Map<Integer, List<? extends Message>> messageListForCurrentSuperstep;
	protected Map<Integer, List<? extends Message>> messageListForNextSuperstep;
	protected Map<Integer, SPVertex<M>> weightedGraph;
	protected List<Pair<Integer, Integer>> numVerticesAndEdgesStats = new ArrayList<Pair<Integer,Integer>>();
	
	public SequentialPregel(Map<Integer, SPVertex<M>> weightedGraph) {
		this.weightedGraph = weightedGraph;
		this.messageListForCurrentSuperstep = new HashMap<Integer, List<? extends Message>>();
		this.messageListForNextSuperstep = new HashMap<Integer, List<? extends Message>>();
		int numEdges = 0;
		int numWeightedEdges = 0;
		for (int vertexId : weightedGraph.keySet()) {
			this.messageListForCurrentSuperstep.put(vertexId, new ArrayList<Message>());
			this.messageListForNextSuperstep.put(vertexId, new ArrayList<Message>());
			numEdges += weightedGraph.get(vertexId).weightedNeighbors.size();
			numWeightedEdges += PartitionerUtils.countWeightedEdges(weightedGraph.get(vertexId));
		}
		System.out.println("numVertices: " + weightedGraph.size());
		System.out.println("numEdges: " + numEdges);
		System.out.println("numWeightedEdges: " + numWeightedEdges);
		numVerticesAndEdgesStats.add(Pair.of(weightedGraph.size(), numEdges));
	}

	protected abstract State getState(int vertexId);

	protected void swapCurrentAndNextMessageLists() {
		Map<Integer, List<? extends Message>> tmpMessageList = messageListForCurrentSuperstep;
		messageListForCurrentSuperstep = messageListForNextSuperstep;
		messageListForNextSuperstep = tmpMessageList;
		for (Entry<Integer, List<? extends Message>> entry : messageListForNextSuperstep.entrySet()) {
			entry.getValue().clear();
		}
	}

//	protected void dumpStatesAndCurrentGraphStats() {
//		System.out.println("Dumping states and current graph stats");
//		List<Integer> vertices = new ArrayList<Integer>(statesMap.keySet());
//		Collections.sort(vertices);
//		int numVertices = 0;
//		Map<Integer, HashSet<Integer>> coarsenedGraph = new HashMap<Integer, HashSet<Integer>>();
//		int numEdgesWithinCluster = 0;
//		for (int vertexId : vertices) {
//			int superNodeId = statesMap.get(vertexId).superNodeId;
////			System.out.println("vertexId: " + vertexId + " superNodeId: "
////				+ superNodeId);
//			if (superNodeId == vertexId) {
//				numVertices++;
//			}
//			if (!coarsenedGraph.containsKey(superNodeId)) {
//				coarsenedGraph.put(superNodeId, new HashSet<Integer>());
//			}
//			HashSet<Integer> coarsenedGraphNeighbors = coarsenedGraph.get(superNodeId);
//			Map<Integer, Integer> weightedNeighbors = weightedGraph.get(vertexId).weightedNeighbors;
//			for (int neighborId : weightedNeighbors.keySet()) {
//				int neighborsSuperNodeId = statesMap.get(neighborId).superNodeId;
//				if (neighborsSuperNodeId != superNodeId) {
//					coarsenedGraphNeighbors.add(neighborsSuperNodeId);
//				} else {
//					numEdgesWithinCluster++;
//				}
//			}
//		}
//		int coarsenedNumEdges = 0;
//		for (Entry<Integer, HashSet<Integer>> entry : coarsenedGraph.entrySet()) {
//			coarsenedNumEdges += entry.getValue().size();
//		}
//		assert (numVertices == coarsenedGraph.size()) : "CoarsenedGraph.size(): "
//			+ coarsenedGraph.size() + " numVertices: " + numVertices;
//		System.out.println("numVertices: " + coarsenedGraph.size());
//		System.out.println("numEdges: " + coarsenedNumEdges);
//		System.out.println("numEdgesWithinClusters: " + numEdgesWithinCluster);
//		numVerticesAndEdgesStats.add(Pair.of(coarsenedGraph.size(), coarsenedNumEdges));
//	}

	protected void dumpNumVerticesAndEdgesStats() {
		for (Pair<Integer, Integer> numVerticesNumEdges : numVerticesAndEdgesStats) {
			System.out.println("Iteration: 0. numVertices: " + numVerticesNumEdges.fst
				+ " numEdges: " + numVerticesNumEdges.snd);
		}
		System.out.println("Starting to print numVertices:");
		for (Pair<Integer, Integer> numVerticesNumEdges : numVerticesAndEdgesStats) {
			System.out.println(numVerticesNumEdges.fst);
		}
		System.out.println("Starting to print numEdges:");
		for (Pair<Integer, Integer> numVerticesNumEdges : numVerticesAndEdgesStats) {
			System.out.println(numVerticesNumEdges.snd);
		}
	}

	public static class State {
		protected int superNodeId;
		protected boolean skip;

		public State(int vertexId) {
			this.superNodeId = vertexId;
			this.skip = false;
		}
	}
	
	public static class Message {
		
	}
}
