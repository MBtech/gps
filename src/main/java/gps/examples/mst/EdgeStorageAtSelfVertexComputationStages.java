package gps.examples.mst;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cern.colt.map.OpenIntIntHashMap;

import gps.examples.mst.MSTComputationStage.ComputationStage;
import gps.examples.mst.MSTVertex.MinEdgeValues;
import gps.examples.mst.MSTVertexValue.MSTVertexType;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.Edge;

public class EdgeStorageAtSelfVertexComputationStages {

	private static Map<Integer, Integer> tmpIntIntMap = new HashMap<Integer, Integer>();
	public static List<Integer> tmpIntList = new ArrayList<Integer>();
	public static OpenIntIntHashMap intIntMap = new OpenIntIntHashMap();
	private static MSTVertexValue value;
	
	public static boolean executeComputationStage(MSTVertex vertex, Iterable<MSTMessageValue> messageValues,
		ComputationStage computationStage) {
		value = vertex.getValue();
		if (MSTVertexType.CERTAINLY_INACTIVE_VERTEX == value.type) {
			vertex.voteToHalt();
			return true;
		}
		switch(computationStage) {
		case MIN_EDGE_PICKING_1:
			doMinEdgePicking1Computation(vertex);
			return true;
		case MIN_EDGE_PICKING_2:
			doMinEdgePicking2Computation(vertex, messageValues);
			return true;
		case NOTIFY_NEW_ROOT_THAT_SELF_IS_A_SUBVERTEX:
			doNotifyRootThatSelfIsASubvertexComputation(vertex);
			return true;
		case NOTIFY_SUBVERTICES_OF_NEW_ROOT_1:
			doNotifySubVerticesOfNewRoot1Computation(vertex, messageValues);
			return true;
		case NOTIFY_SUBVERTICES_OF_NEW_ROOT_2:
			doNotifySubVerticesOfNewRoot2Computation(vertex, messageValues);
			return true;
		case AT_SELF_EDGE_CLEANING_1:
			doEdgeCleaning1Computation(vertex);
			return true;
		case AT_SELF_EDGE_CLEANING_2:
			doEdgeCleaning2Computation(vertex, messageValues);
			return true;
		case ECOD_MIN_EDGE_PICKING_QUESTION:
			doECODMinEdgePickingQuestionComputation(vertex, messageValues);
			return true;
		case ECOD_MIN_EDGE_PICKING_ANSWER_SENDING:
			doECODMinEdgePickingAnswerSendingComputation(vertex, messageValues);
			return true;
		case ECOD_MIN_EDGE_PICKING_ANSWER_RECEIVING:
			doECODMinEdgePickingAnswerReceivingComputation(vertex, messageValues);
			return true;
		case ECOD_EDGE_CLEANING_1:
			doECODEdgeCleaning1Computation(vertex, messageValues);
			return true;
		case ECOD_EDGE_CLEANING_2:
			doECODEdgeCleaning2Computation(vertex, messageValues);
			return true;
		case ECOD_EDGE_CLEANING_3:
			doECODEdgeCleaning3Computation(vertex, messageValues);
			return true;
		default:
			return false;
		}
	}

	private static void doECODMinEdgePickingQuestionComputation(MSTVertex vertex,
		Iterable<MSTMessageValue> messageValues) {
		if (value.type == MSTVertexType.SUB_VERTEX_PICKED_MIN_EDGE
			|| value.type == MSTVertexType.ROOT_PICKED_MIN_EDGE) {
			return;
		}
		if (vertex.options.isFirstECODMinEdgePickingQuestion) {
			relabelTheLatestPickedEdgeToNewId(vertex, -1, false);
		}
		MinEdgeValues minEdge = vertex.findMinEdge();
		if (minEdge.pickedEdgeId < 0 || minEdge.pickedEdgeOriginalFromId < 0
			|| minEdge.pickedEdgeOriginalToId < 0) {
			if (MSTVertexType.SUB_VERTEX == value.type
				|| MSTVertexType.SUB_VERTEX_NOT_PICKED_MIN_EDGE == value.type) {
				voteToHaltAndSetTypeToCertainlyConverged(vertex);
				return;
			} else if (MSTVertexType.ROOT == value.type
				|| MSTVertexType.ROOT_NOT_PICKED_MIN_EDGE == value.type) {
				return;
			}
		}
		if (minEdge.pickedEdgeId == value.parent || 
			minEdge.pickedEdgeOriginalToId == value.parent ||
			minEdge.equalWeightPick) {
			// minEdge.edgeWeightPick;
			// don't send anything to parents or when the pick is done through equalWeight.
			// we avoid equal weight picks because we are doing edge cleaning on demand,
			// may not know the actual ids and pick a wrong min edge to ask.
			return;
		}
		vertex.sendMessage(minEdge.pickedEdgeOriginalToId,
			MSTMessageValue.newECODQuestionMessage(vertex.getId(), value.parent));
	}

	private static void doECODMinEdgePickingAnswerSendingComputation(MSTVertex vertex,
		Iterable<MSTMessageValue> messageValues) {
		for (MSTMessageValue message : messageValues) {
			if (message.int2 != value.parent) {
				vertex.sendMessage(message.int1, MSTMessageValue.newECODAnswerMessage(value.parent));
			}
		}
	}

	private static void doECODMinEdgePickingAnswerReceivingComputation(MSTVertex vertex,
		Iterable<MSTMessageValue> messageValues) {
		if (value.type == MSTVertexType.SUB_VERTEX_PICKED_MIN_EDGE
			|| value.type == MSTVertexType.ROOT_PICKED_MIN_EDGE) {
			return;
		}
		if (messageValues.iterator().hasNext()) {
			// We don't have to do anything. We know that our immediate min-edge is correct
			// System.out.println("increasing success ecod by 1");
			vertex.globalObjectsMap.putOrUpdateGlobalObject("num-succ-ecod", new IntSumGlobalObject(1));
			int newId = messageValues.iterator().next().int1;
			relabelTheLatestPickedEdgeToNewId(vertex, newId, false);
			if (MSTVertexType.SUB_VERTEX == value.type
				|| MSTVertexType.SUB_VERTEX_NOT_PICKED_MIN_EDGE == value.type) {
				value.type = MSTVertexType.SUB_VERTEX_PICKED_MIN_EDGE;
			} else if (MSTVertexType.ROOT == value.type ||
				MSTVertexType.ROOT_NOT_PICKED_MIN_EDGE == value.type) {
				value.type = MSTVertexType.ROOT_PICKED_MIN_EDGE;
			}
		} else {
			relabelTheLatestPickedEdgeToNewId(vertex, -1, true);
			vertex.globalObjectsMap.putOrUpdateGlobalObject("num-fail-ecod", new IntSumGlobalObject(1));
			if (MSTVertexType.SUB_VERTEX == value.type) {
				value.type = MSTVertexType.SUB_VERTEX_NOT_PICKED_MIN_EDGE;
			} else if (MSTVertexType.ROOT == value.type) {
				value.type = MSTVertexType.ROOT_NOT_PICKED_MIN_EDGE;
			}
		}
	}
	
	private static void doECODEdgeCleaning1Computation(MSTVertex vertex,
		Iterable<MSTMessageValue> messageValues) {
		if (MSTVertexType.SUB_VERTEX_NOT_PICKED_MIN_EDGE == value.type
			|| MSTVertexType.ROOT_NOT_PICKED_MIN_EDGE == value.type) {
			MSTMessageValue messageToSend = MSTMessageValue.newECODEdgeCleaning1Message(vertex.getId());
			for (Edge<MSTEdgeValue> edge : vertex.getOutgoingEdges()) {
				if (edge.getNeighborId() >= 0) {
					vertex.sendMessage(edge.getEdgeValue().originalToId, messageToSend);
				}
			}
		}
	}

	private static void doECODEdgeCleaning2Computation(MSTVertex vertex,
		Iterable<MSTMessageValue> messageValues) {
		for (MSTMessageValue message : messageValues) {
			vertex.sendMessage(message.int1, MSTMessageValue.newECODEdgeCleaning2Message(vertex.getId(),
				value.parent));
		}
	}

	private static void doECODEdgeCleaning3Computation(MSTVertex vertex, Iterable<MSTMessageValue> messageValues) {
		if (value.type == MSTVertexType.SUB_VERTEX_NOT_PICKED_MIN_EDGE
			|| value.type == MSTVertexType.ROOT_NOT_PICKED_MIN_EDGE) {
			if (messageValues.iterator().hasNext()) {
				intIntMap.clear();
				for (MSTMessageValue messageValue : messageValues) {
					if (value.parent != messageValue.int2) {
						intIntMap.put(messageValue.int1, messageValue.int2);
					} else {
						intIntMap.put(messageValue.int1, -1);
					}
				}
			}
			int tmpNewId;
			int neighborIndex = -1;
			for (Edge<MSTEdgeValue> edge : vertex.getOutgoingEdges()) {
				neighborIndex++;
				if (edge.getNeighborId() >= 0) {
					if (!intIntMap.containsKey(edge.getEdgeValue().originalToId)) {
						tmpNewId = -1;
					} else {
						tmpNewId = intIntMap.get(edge.getEdgeValue().originalToId);
					}
					vertex.relabelIdOfNeighbor(neighborIndex, tmpNewId);
				}
			}
		}

		if (value.type == MSTVertexType.ROOT
			|| value.type == MSTVertexType.ROOT_NOT_PICKED_MIN_EDGE
			|| value.type == MSTVertexType.ROOT_PICKED_MIN_EDGE) {
			value.type = MSTVertexType.POINTS_AT_NONROOT_VERTEX;
		} else {
			value.type = MSTVertexType.SUB_VERTEX;
		}
	}

	private static void doEdgeCleaning1Computation(MSTVertex vertex) {
		for (Edge<MSTEdgeValue> edge: vertex.getOutgoingEdges()) {
			if (edge.getNeighborId() >= 0) {
				vertex.sendMessage(edge.getEdgeValue().originalToId,
					MSTMessageValue.newSupernodeMessage(vertex.getId(), vertex.getValue().parent));
			}
		}
	}

	private static void doNotifySubVerticesOfNewRoot2Computation(MSTVertex vertex,
		Iterable<MSTMessageValue> messageValues) {
		if (MSTVertexType.SUB_VERTEX != value.type && messageValues.iterator().hasNext()) {
			vertex.printToStdErrAndThrowARuntimeException("ERROR!!! vertex with id: " + vertex.getId() +
				" is not of type SUB_VERTEX and has received a message in stage NOTIFY_SUBVERTICES_OF_NEW_ROOT_2" +
				" type: " + value.type + " parent: " + value.parent);
		}
		if (MSTVertexType.SUB_VERTEX != value.type) {
			if (vertex.options.isECOD && MSTVertexType.POINTS_AT_ROOT == value.type) {
				if (vertex.getId() == value.parent) {
					value.type = MSTVertexType.ROOT;
				} else {
					value.type = MSTVertexType.SUB_VERTEX;
				}
			}
			return;
		}
		int numMessages = 0;
		for (MSTMessageValue messageValue : messageValues) {
			value.parent = messageValue.int1;
			numMessages++;
		}
		
		if (numMessages > 1) {
			vertex.printToStdErrAndThrowARuntimeException("ERROR!!! vertex with id: " + vertex.getId() +
				" is SUB_VERTEX but has received " + numMessages + " in stage NOTIFY_SUBVERTICES_OF_NEW_ROOT_2");
		}
	}

	private static void doNotifySubVerticesOfNewRoot1Computation(MSTVertex vertex,
		Iterable<MSTMessageValue> messageValues) {
		for (MSTMessageValue messageValue : messageValues) {
			vertex.sendMessage(messageValue.int1, MSTMessageValue.newParentMessage(value.parent));
		}
	}

	private static void doNotifyRootThatSelfIsASubvertexComputation(MSTVertex vertex) {
		if (MSTVertexType.SUB_VERTEX == value.type) {
			vertex.sendMessage(value.parent, MSTMessageValue.newSubvertexMessage(vertex.getId()));
		}
	}

	private static void doEdgeCleaning2Computation(MSTVertex vertex, Iterable<MSTMessageValue> messageValues) {
		int totalNeighbors = 0;
		if (messageValues.iterator().hasNext()) {
			intIntMap.clear();
			for (MSTMessageValue messageValue : messageValues) {
				if (value.parent != messageValue.int2) {
					intIntMap.put(messageValue.int1, messageValue.int2);
				} else {
					intIntMap.put(messageValue.int1, -1);
				}
			}
			int tmpNewId;
			int neighborIndex = -1;
			for (Edge<MSTEdgeValue> edge : vertex.getOutgoingEdges()) {
				neighborIndex++;
				if (edge.getNeighborId() >= 0) {
					tmpNewId = intIntMap.get(edge.getEdgeValue().originalToId);
					if (vertex.options.isECOD && tmpNewId == 0) {
						if (!intIntMap.containsKey(edge.getEdgeValue().originalToId)) {
							tmpNewId = -1;
						}
					}
					vertex.relabelIdOfNeighbor(neighborIndex, tmpNewId);
					if (tmpNewId >= 0) {
						totalNeighbors++;
					}
				}
			}
		}
		vertex.getGlobalObjectsMap().putOrUpdateGlobalObject(
			MSTOptions.GOBJ_NUM_EDGES_FOR_EDGE_STORAGE_AT_SELF_CUSTOM_COUNTING,
			new IntSumGlobalObject(totalNeighbors));
		if (MSTVertexType.ROOT != value.type && totalNeighbors == 0) {
			voteToHaltAndSetTypeToCertainlyConverged(vertex);
			return;
		}
		if (value.type == MSTVertexType.ROOT) {
			value.type = MSTVertexType.POINTS_AT_NONROOT_VERTEX;
		} else {
			value.type = MSTVertexType.SUB_VERTEX;
		}
	}

	private static void voteToHaltAndSetTypeToCertainlyConverged(MSTVertex vertex) {
		vertex.removeEdges();
		vertex.voteToHalt();
		value.type = MSTVertexType.CERTAINLY_INACTIVE_VERTEX;
	}

	private static void doMinEdgePicking2Computation(MSTVertex vertex, Iterable<MSTMessageValue> messageValues) {
		if (MSTVertexType.POINTS_AT_NONROOT_VERTEX != value.type) {
			return;
		}
		MinEdgeValues minEdge = vertex.findMinEdge();
		if (minEdge.pickedEdgeId < 0 && !messageValues.iterator().hasNext()) {
			voteToHaltAndSetTypeToCertainlyConverged(vertex);
			return;
		}
		for (MSTMessageValue messageValue : messageValues) {
			if (messageValue.double1 < minEdge.minWeight) {
				setMinEdgeValueFromMessageValue(minEdge, messageValue);
			} else if (messageValue.double1 == minEdge.minWeight && messageValue.int1 < minEdge.pickedEdgeId) {
				setMinEdgeValueFromMessageValue(minEdge, messageValue);
			}
		}
		vertex.doMinEdgeComputation(minEdge);
	}

	private static void setMinEdgeValueFromMessageValue(MinEdgeValues minEdge,
		MSTMessageValue messageValue) {
		minEdge.pickedEdgeId = messageValue.int1;
		minEdge.pickedEdgeOriginalFromId = messageValue.int2;
		minEdge.pickedEdgeOriginalToId = messageValue.int3;
		minEdge.minWeight = messageValue.double1;
	}

	private static void doMinEdgePicking1Computation(MSTVertex vertex) {
		if (MSTVertexType.SUB_VERTEX == vertex.getValue().type) {
			MinEdgeValues minEdge = vertex.findMinEdge();
			if (minEdge.pickedEdgeId < 0) {
				voteToHaltAndSetTypeToCertainlyConverged(vertex);
				return;
			}
			vertex.sendMessage(value.parent, MSTMessageValue.newEdgeMessage(minEdge.pickedEdgeId,
				minEdge.pickedEdgeOriginalFromId, minEdge.pickedEdgeOriginalToId, minEdge.minWeight));
		}
	}

	private static void relabelTheLatestPickedEdgeToNewId(MSTVertex vertex, int newId, boolean skipIfEqualWeightPick) {
		MinEdgeValues minEdge = vertex.findMinEdge();
		if (skipIfEqualWeightPick && minEdge.equalWeightPick) {
			return;
		}
		if (minEdge.localNeighborIndex >= 0) {
			vertex.relabelIdOfNeighbor(minEdge.localNeighborIndex, newId);
		}
	}

	protected static Map<Integer, Integer> getNewNeighborIds(Iterable<MSTMessageValue> messageValues) {
		tmpIntIntMap.clear();
		for (MSTMessageValue messageValue : messageValues) {
			tmpIntIntMap.put(messageValue.int1, messageValue.int2);
		}
		return tmpIntIntMap;
	}
}