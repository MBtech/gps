package gps.examples.mst;

import java.util.HashMap;

import gps.examples.mst.MSTComputationStage.ComputationStage;
import gps.examples.mst.MSTVertexValue.MSTVertexType;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.Edge;

/**
 * Contains the logic for the different vertex-centric computation phases when the edges 
 * are always stored at the supernodes. The alternative is storing the edges always at self.
 * 
 * @author semihsalihoglu
 */
public class EdgeStorageAtRootVertexComputationStages {

	private static HashMap<Integer, MSTMessageValue> tmpMessageValueMap =
		new HashMap<Integer, MSTMessageValue>();

	/*
	 * Stages of the Edge Cleaning Phase:
	 * Superstep 1: (a) Every vertex v sends for each of its neighbors: a new edge, now instead pointing to
	 *               v's parent (possibly v itself), original from id, original to id and weight.
	 *              (b) Every vertex removes its edges
	 * Superstep 2: Every ROOT vertex r adds every new edge with toId = u that it receives if it is to
	 *              a vertex u != r.
	 *              Every NON_ROOT vertex n sends every new edge with toId = u  that it receives if it is to
	 *              a vertex u != n.parent.     
	 * Superste 3: Only the roots receive messages. Every received message is added as an edge.
	 */
	public static boolean executeComputationStage(MSTVertex vertex, Iterable<MSTMessageValue> messageValues,
		ComputationStage computationStage) {
		switch(computationStage) {
		case AT_ROOT_EDGE_CLEANING_1:
			vertex.readAnswerMessagesAndVerifyAtMostOneAnswerMessage(messageValues,
				true /* is edge cleaning */);
			for (Edge<MSTEdgeValue> outgoingEdge : vertex.getOutgoingEdges()) {
				if (vertex.getValue().parent == outgoingEdge.getNeighborId()) {
					vertex.getGlobalObjectsMap().putOrUpdateGlobalObject(
						MSTOptions.GOBJ_NUM_EDGES_REMOVED_IN_EDGE_CLEANING_1, new IntSumGlobalObject(1));
					continue;
				}
				if (outgoingEdge.getNeighborId() >= 0) {
					vertex.sendMessage(outgoingEdge.getNeighborId(), MSTMessageValue.newEdgeMessage(
						vertex.getValue().parent,
					outgoingEdge.getEdgeValue().originalFromId,
					outgoingEdge.getEdgeValue().originalToId,
					outgoingEdge.getEdgeValue().weight));
				}
			}
			vertex.removeEdges();
			return true;
		case AT_ROOT_EDGE_CLEANING_2:
			runEdgeCleaningStep2(vertex, messageValues);
			return true;
		case AT_ROOT_EDGE_CLEANING_3:
			runEdgeCleaningStep3(vertex, messageValues);
			return true;
		default:
			return false;
		}
	}

	private static void runEdgeCleaningStep2(MSTVertex vertex, Iterable<MSTMessageValue> messageValues) {
		if (vertex.options.useHashMapInEdgeCleaningForEdgesAtRoot
			&& vertex.getNeighborsSize() <= vertex.options.numEdgesThresholdForUsingHashMap) {
			runEdgeCleaning2WithHashMap(vertex, messageValues);
		} else {
			for (MSTMessageValue messageValue : messageValues) {
				addEdgeOrSendAMessageForEdgeCleaning2Message(vertex, messageValue);
			}
		}
		if (MSTVertexType.ROOT != vertex.getValue().type) {
			vertex.voteToHalt();
		}
	}

	private static void addEdgeOrSendAMessageForEdgeCleaning2Message(MSTVertex vertex,
		MSTMessageValue messageValue) {
		if (MSTVertexType.ROOT == vertex.getValue().type) {
			vertex.addMSTVertexEdge(messageValue.int1, messageValue.int2,
				messageValue.int3, messageValue.double1);
		} else {
			if (vertex.getValue().parent != messageValue.int1) {
				vertex.sendMessage(vertex.getValue().parent, messageValue);
			} else {
				vertex.getGlobalObjectsMap().putOrUpdateGlobalObject(
					MSTOptions.GOBJ_NUM_EDGES_REMOVED_IN_EDGE_CLEANING_2,
					new IntSumGlobalObject(1));
			}
		}
	}

	private static void runEdgeCleaning2WithHashMap(MSTVertex vertex,
		Iterable<MSTMessageValue> messageValues) {
		tmpMessageValueMap.clear();
		MSTMessageValue tmpMessageWrapper;
		for (MSTMessageValue messageValue : messageValues) {
			tmpMessageWrapper = tmpMessageValueMap.get(messageValue.int1);
			if (tmpMessageWrapper == null || messageValue.double1 < tmpMessageWrapper.double1) {
				tmpMessageValueMap.put(messageValue.int1, MSTMessageValue.newEdgeMessage(
					messageValue.int1, messageValue.int2, messageValue.int3, messageValue.double1));
			}
		}
		for (MSTMessageValue messageValue : tmpMessageValueMap.values()) {
			addEdgeOrSendAMessageForEdgeCleaning2Message(vertex, messageValue);
		}
	}

	private static void runEdgeCleaningStep3(MSTVertex vertex, Iterable<MSTMessageValue> messageValues) {
		if (MSTVertexType.ROOT != vertex.getValue().type) {
			System.err.println("ERROR!!! In the 3rd edge cleaning step the only vertices" +
				" that are active are the ROOTs. vertexType: " + vertex.getValue().type);
			throw new RuntimeException("ERROR!!! In the 3rd edge cleaning step the only vertices" +
				" that are active are the ROOTs. ");
		}
		// Because only the ROOT condition will execute.
		runEdgeCleaningStep2(vertex, messageValues);
		if (vertex.getNeighborsSize() == 0) {
			vertex.voteToHalt();
		}
	}
}