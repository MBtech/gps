package gps.examples.mis;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;

import gps.examples.mis.MISVertexValue.MISVertexType;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;

public class MISVertex extends NullEdgeVertex<MISVertexValue, MISMessage> {

	private static MISVertexValue value;
	private static Set<Integer> removedNeighbors = new HashSet<Integer>();
	public MISVertex(CommandLine line) {
	}

	@Override
	public void compute(Iterable<MISMessage> messageValues, int superstepNo) {
		value = getValue();
		if (superstepNo == 1 && getNeighborsSize() == 0) {
			value.type = MISVertexType.IN_SET;
			voteToHalt();
		}
		// System.out.println("computing vertex id: " + getId() + " type: " + value.type);
		if (MISVertexType.IN_SET == value.type || MISVertexType.NOT_IN_SET == value.type) {
			// System.out.println("vertex is returning because type is: " + value.type);
			voteToHalt();
			return;
		}
		getGlobalObjectsMap().putOrUpdateGlobalObject("num-not-converged",
			new IntSumGlobalObject(1));
		if ((superstepNo % 4) == 3 || (superstepNo % 4) == 0) {
			errorIfVertexTypeIsSelectedAsPossibleInSet();
		}
		if (superstepNo == 1) {
			value.numRemainingNeighbors = getNeighborsSize();
		}
		if ((superstepNo % 4) == 1) {
			if (MISVertexType.IN_SET == value.type) {
				voteToHalt();
				System.err.println("ERROR!!! vertex with id: " + getId()
					+ " should not be active because it already is in the MIS. superstepNo: "
					+ superstepNo);
			}
			double probability = getNeighborsSize() > 0 ? 1.0 / ((double) 2*value.numRemainingNeighbors) : -1;
			if (Math.random() < probability) {
				value.type = MISVertexType.SELECTED_AS_POSSIBLE_IN_SET;
				MISMessage newSelectedAsPossibleMessage = MISMessage.newNeighborSelectedAsPossibleMessage(getId());
				for (int neighborId : getNeighborIds()) {
					if (neighborId >= 0) {
						sendMessage(neighborId, newSelectedAsPossibleMessage);
					}
				}
			}
		} else if ((superstepNo % 4) == 2) {
			if (MISVertexType.SELECTED_AS_POSSIBLE_IN_SET == value.type) {
				for (MISMessage message : messageValues) {
					if (message.int1 < getId()) {
						value.type = MISVertexType.UNDECIDED;
						break;
					}
				}
			}
			if (MISVertexType.SELECTED_AS_POSSIBLE_IN_SET == value.type) {
				for (int neighborId : getNeighborIds()) {
					if (neighborId >= 0) {
						sendMessage(neighborId, MISMessage.newHasANeighborInSetMessage());
					}
				}
				setTypeRemoveEdgesAndVoteToHalt(MISVertexType.IN_SET);
			}
		} else if ((superstepNo % 4) == 3) {
			if (messageValues.iterator().hasNext()) {
				if (MISVertexType.NOT_IN_SET == value.type || MISVertexType.IN_SET == value.type) {
					printToStdErrAndThrowARuntimeException("Vertex with id: " + getId() + " has type: "
						+ value.type + " and has messages in phase 3 where vertices receive" +
						" messages from neighbors who have put them in their sets.");
				}
				MISMessage removeNeighborMessage = MISMessage.removeNeighborMessage(getId());
				for (int neighborId : getNeighborIds()) {
					if (neighborId >= 0) {
						sendMessage(neighborId, removeNeighborMessage);
					}
				}
				setTypeRemoveEdgesAndVoteToHalt(MISVertexType.NOT_IN_SET);
			}
		}  else if ((superstepNo % 4) == 0) {
			if (messageValues.iterator().hasNext()) {
				removedNeighbors.clear();
				for (MISMessage message : messageValues) {
					removedNeighbors.add(message.int1);
				}
				int numNeighborsRemoved = 0;
				int neighborIdIndex = 0;
				for (int neighborId : getNeighborIds()) {
					if (neighborId >= 0 && removedNeighbors.contains(neighborId)) {
						numNeighborsRemoved++;
						relabelIdOfNeighbor(neighborIdIndex, -1);
					}
					neighborIdIndex++;
				}
				// System.out.println("numNeighborsRemoved: " + numNeighborsRemoved);
				value.numRemainingNeighbors -= numNeighborsRemoved;
			}
		}
	}

	private void errorIfVertexTypeIsSelectedAsPossibleInSet() {
		if (MISVertexType.SELECTED_AS_POSSIBLE_IN_SET == value.type) {
			printToStdErrAndThrowARuntimeException("in phase 3 or 0 there cannot be any SELECTED_AS_POSSIBLE_IN_SET vertices.");
		}
	}

	private void setTypeRemoveEdgesAndVoteToHalt(MISVertexType vertexType) {
		value.type = vertexType;
		removeEdges();
		voteToHalt();
	}

	@Override
	public MISVertexValue getInitialValue(int id) {
		return new MISVertexValue();
	}
	
	/**
	 * Factory class for {@link MISVertex}.
	 * 
	 * @author semihsalihoglu
	 */
	public static class MISVertexFactory
		extends NullEdgeVertexFactory<MISVertexValue, MISMessage> {

		@Override
		public NullEdgeVertex<MISVertexValue, MISMessage> newInstance(CommandLine commandLine) {
			return new MISVertex(commandLine);
		}
	}
}