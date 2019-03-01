package gps.examples.mst;

import org.apache.commons.cli.CommandLine;

import gps.examples.mst.MSTComputationStage.ComputationStage;
import gps.examples.mst.MSTMessageValue.MSTMessageType;
import gps.examples.mst.MSTVertexValue.MSTVertexType;
import gps.globalobjects.DoubleSumGlobalObject;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.Edge;
import gps.graph.Vertex;
import gps.writable.IntWritable;

/**
 * Vertex-centric implementation of Boruvka's MST algorithm.
 * Stages:
 * 
 * @author semihsalihoglu
 */
public abstract class MSTVertex extends Vertex<MSTVertexValue, MSTEdgeValue, MSTMessageValue> {

	protected MSTOptions options;
	protected static int superstepGlobalMapsAreFrom = -1;
	protected MSTVertexValue value;
	private static ComputationStage computationStage;
	private static MSTEdgeValue tmpEdge = new MSTEdgeValue();
	public MSTVertex(CommandLine commandLine) {
		options = new MSTOptions();
		options.parseOtherOpts(commandLine);
	}

	@Override
	public void compute(Iterable<MSTMessageValue> messageValues, int superstepNo) {
		if (superstepGlobalMapsAreFrom < superstepNo) {
			setGlobalVariablesAndDataStructures();
			superstepGlobalMapsAreFrom = superstepNo;
		}
		if (superstepNo == 1) {
			setValue(new MSTVertexValue());
			initializeTypeToPointsAtNonRoot();
		}
		value = getValue();
		if (checkIfSingletonOrErrorVertex(superstepNo)) {
			return;
		}
		setDefaultGlobalObjects();
		switch(computationStage) {
		case IMMEDIATE_MIN_EDGE_PICKING:
			doMinEdgeComputation(findMinEdge());
			break;
		case POINTER_JUMPING_QUESTION_1:
			doPointerJumpingComputation(messageValues);
			break;
		case POINTER_JUMPING_QUESTION:
			if (value.type == MSTVertexType.POINTS_AT_NONROOT_VERTEX
				&& !messageValues.iterator().hasNext()) {
				System.err.println("ERROR!!! id: " + getId() + " is setting itself to an error vertex."); 
				value.type = MSTVertexType.ERROR_VERTEX;
				return;
			}
			doPointerJumpingComputation(messageValues);
			break;
		case POINTER_JUMPING_ANSWER:
			// Note: We are looping over messages here but there will actually be exactly 1 message.
			for (MSTMessageValue messageValue : messageValues) {
				if (value.parent == messageValue.int1) {
					if (getId() < value.parent) {
						value.type = MSTVertexType.ROOT;
						value.parent = getId();
						getGlobalObjectsMap().putOrUpdateGlobalObject("negative-tree-weight",
							new DoubleSumGlobalObject(value.pickedEdgeWeight));
					} else {
						value.type = MSTVertexType.POINTS_AT_ROOT;
					}
				} else {
					if (value.type == MSTVertexType.POINTS_AT_NONROOT_VERTEX) {
						sendMessage(messageValue.int1, MSTMessageValue.newParentMessage(value.parent));					
					} else {
						sendMessage(messageValue.int1, MSTMessageValue.newIsRootMessage(value.parent));
					}
				}
			}
			break;
		default:
			computeFurther(messageValues, computationStage);
		}
	}

	public void doMinEdgeComputation(MinEdgeValues minEdge) {
		initializeTypeToPointsAtNonRoot();
		setValueFieldsToPickedMinEdge(minEdge);
	}

	private void setValueFieldsToPickedMinEdge(MinEdgeValues minEdge) {
		if (getId() == 7 || getId() == 128) {
			System.out.println("vertex " + getId() + " is picking edge: " + minEdge.pickedEdgeOriginalFromId + "-" +
				minEdge.pickedEdgeOriginalToId);
		}
//		if (minEdge.pickedEdgeOriginalFromId == 4 && minEdge.pickedEdgeOriginalToId == 26) {
//			System.out.println("vertex " + getId() + " is picking 4-26");
//		} else if (minEdge.pickedEdgeOriginalFromId == 26 && minEdge.pickedEdgeOriginalToId == 4) {
//			System.out.println("vertex " + getId() + " is picking 26-4");
//		}
		value.pickedEdgeOriginalFromId = minEdge.pickedEdgeOriginalFromId <= -1 ? getId() :
			minEdge.pickedEdgeOriginalFromId;
		value.pickedEdgeOriginalToId = minEdge.pickedEdgeOriginalToId <= -1
			? minEdge.pickedEdgeId : minEdge.pickedEdgeOriginalToId;
//		vertex.getValue().parent = minEdge.pickedEdgeId;
		getGlobalObjectsMap().putOrUpdateGlobalObject("positive-tree-weight",
			new DoubleSumGlobalObject(minEdge.minWeight));
		value.parent = minEdge.pickedEdgeId;
		value.pickedEdgeWeight = minEdge.minWeight;
	}

	protected void initializeTypeToPointsAtNonRoot() {
		getValue().type = MSTVertexType.POINTS_AT_NONROOT_VERTEX;
	}

	private void setDefaultGlobalObjects() {
		getGlobalObjectsMap().putOrUpdateGlobalObject(MSTOptions.GOBJ_NUM_EDGES_CUSTOM_COUNTING,
			new IntSumGlobalObject(getNeighborsSize()));
		getGlobalObjectsMap().putOrUpdateGlobalObject(MSTOptions.GOBJ_NUM_VERTICES_CUSTOM_COUNTING,
			new IntSumGlobalObject(1));
		if (MSTVertexType.ROOT == value.type) {
			getGlobalObjectsMap().putOrUpdateGlobalObject(MSTOptions.GOBJ_NUM_SUPERNODES,
				new IntSumGlobalObject(1));
		}
	}

	private boolean checkIfSingletonOrErrorVertex(int superstepNo) {
		if (getNeighborsSize() == 0 && superstepNo == 1) {
			System.out.println("vertex with id: " + getId() + " is a singleton in the first superstep." +
				" so setting it to an error vertex");
			getGlobalObjectsMap().putOrUpdateGlobalObject("singletons", new IntSumGlobalObject(1));
			value.type = MSTVertexType.ERROR_VERTEX;
			voteToHalt();
			return true;
		}
		if (value == null) {
			System.err.println("ERROR!!! vertex id: " + getId() + " has null vertex value. numNeighbors: "
				+ getNeighborsSize());
			return true;
		}
		if (value.type == MSTVertexType.ERROR_VERTEX) {
			System.err.println("id: " + getId() + " is an error vertex.");
			getGlobalObjectsMap().putOrUpdateGlobalObject(MSTOptions.GOBJ_ERROR_VERTICES,
				new IntSumGlobalObject(1));
			voteToHalt();
			return true;
		}
		return false;
	}

	private void doPointerJumpingComputation(Iterable<MSTMessageValue> messageValues) {
		readAnswerMessagesAndVerifyAtMostOneAnswerMessage(messageValues,
			false /* not edge cleaning step */);
		if (value.type == MSTVertexType.POINTS_AT_NONROOT_VERTEX && getValue().parent >= 0) {
			sendMessage(value.parent, MSTMessageValue.newPointingAtYouMessage(getId()));
			getGlobalObjectsMap().putOrUpdateGlobalObject(MSTOptions.GOBJ_POINTING_AT_NON_ROOT,
				new IntSumGlobalObject(1));
		}
	}

	public void readAnswerMessagesAndVerifyAtMostOneAnswerMessage(
		Iterable<MSTMessageValue> messageValues, boolean isEdgeCleaningStep) {
		int numMessages = 0;
		for (MSTMessageValue messageValue : messageValues) {
			numMessages++;
			value.parent = messageValue.int1;
			if (value.parent == getId()) {
				System.err.println("ERROR!!! Pointing at self! id: " + getId());
			}
			if (MSTMessageType.IS_ROOT_MESSAGE == messageValue.type) {
				if (isEdgeCleaningStep) {
					getGlobalObjectsMap().putOrUpdateGlobalObject("num-roots-discovered-in-edge-cleaning",
						new IntSumGlobalObject(1));
				}
				value.type = MSTVertexType.POINTS_AT_ROOT;
			} else if (MSTMessageType.NEW_PARENT_MESSAGE != messageValue.type) {
				System.err.println("ERROR!!! In POINTER_JUMPING_QUESTION stage, only" +
					" IS_ROOT_MESSAGE or NEW_PARENT_MESSAGE can be received. Received message type: " +
					messageValue.type);
			}
		}
		if (numMessages > 1) {
			throw new RuntimeException("ERROR!!! In POINTER_JUMPING_QUESTION stage, num messages can either be " +
				"0 or 1. numMessages: " + numMessages);
		}
	}

	abstract protected void computeFurther(Iterable<MSTMessageValue> messageValues,
		ComputationStage computationStage);

	protected void addMSTVertexEdge(int newToId, int originalFromId, int originalToId, double weight) {
		tmpEdge.originalFromId = originalFromId;
		tmpEdge.originalToId = originalToId;
		tmpEdge.weight = weight;
//		addEdges(new int[] {newToId}, new MSTEdgeValue[] {newEdgeValue});
		addEdge(newToId, tmpEdge);
	}

	protected MinEdgeValues findMinEdge() {
		MinEdgeValues minEdgeValues = new MinEdgeValues();
		double tmpWeight;
		int localNeighborIndex = -1;
		for (Edge<MSTEdgeValue> edge : getOutgoingEdges()) {
			localNeighborIndex++;
			if (edge.getNeighborId() < 0) {
				continue;
			}
			tmpWeight = edge.getEdgeValue().weight;
			if (minEdgeValues.pickedEdgeId == -1) {
				minEdgeValues.pickedEdgeId = edge.getNeighborId();
				minEdgeValues.pickedEdgeOriginalFromId = edge.getEdgeValue().originalFromId;
				minEdgeValues.pickedEdgeOriginalToId = edge.getEdgeValue().originalToId;
				minEdgeValues.minWeight = tmpWeight;
				minEdgeValues.localNeighborIndex = localNeighborIndex;
				minEdgeValues.equalWeightPick = false;
				continue;
			}
			if (tmpWeight < minEdgeValues.minWeight) {
				minEdgeValues.equalWeightPick = false;
				setMinEdgeValue(minEdgeValues, tmpWeight, edge, localNeighborIndex);
			} else if (tmpWeight == minEdgeValues.minWeight) {
				minEdgeValues.equalWeightPick = true;
				if (edge.getNeighborId() < minEdgeValues.pickedEdgeId) {
					setMinEdgeValue(minEdgeValues, tmpWeight, edge, localNeighborIndex);
				}
			}
		}
		return minEdgeValues;
	}

	private void setMinEdgeValue(MinEdgeValues minEdgeValues, double tmpWeight,
		Edge<MSTEdgeValue> edge, int localNeighborIndex) {
		minEdgeValues.minWeight = tmpWeight;
		minEdgeValues.pickedEdgeOriginalFromId = edge.getEdgeValue().originalFromId;
		minEdgeValues.pickedEdgeOriginalToId = edge.getEdgeValue().originalToId;
		minEdgeValues.pickedEdgeId = edge.getNeighborId();
		minEdgeValues.localNeighborIndex = localNeighborIndex;
	}

	private void setGlobalVariablesAndDataStructures() {
		computationStage = ComputationStage.getComputationStageFromId(
			((IntWritable) getGlobalObjectsMap().getGlobalObject(
			MSTOptions.GOBJ_COMP_STAGE).getValue()).getValue());
		// The below computation is done only for the ECOD optimization.
		// We need to distinguish between the first time ECOD_MIN_EDGE_PICKING_QUESTION
		// is happening because the computation starts with IMMEDIATE_MIN_EDGE_PICKING
		// and as a result in the first ECOD_MIN_EDGE_PICKING all vertices fail to find
		// a successful match. They just pick what they picked in IMMEDIATE_MIN_EDGE_PICKING
		if (computationStage == ComputationStage.ECOD_MIN_EDGE_PICKING_ANSWER_SENDING &&
			options.isFirstECODMinEdgePickingQuestion) {
			options.isFirstECODMinEdgePickingQuestion = false;
		}
	}

	protected static class MinEdgeValues {
		int pickedEdgeId = -1;
		int pickedEdgeOriginalFromId = -1;
		int pickedEdgeOriginalToId = -1;
		double minWeight = Double.MAX_VALUE;
		int localNeighborIndex = -1;
		boolean equalWeightPick = false;
	}
}