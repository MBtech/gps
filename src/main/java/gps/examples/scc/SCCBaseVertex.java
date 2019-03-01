package gps.examples.scc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;

import gps.examples.scc.SCCComputationPhase.Phase;
import gps.examples.scc.SCCVertexValue.SCCVertexType;
import gps.examples.scc.gobj.GraphGObj;
import gps.examples.scc.gobj.NodeWritable;
import gps.globalobjects.IntegerIntegerSumMapGObj;
import gps.globalobjects.LongSumGlobalObject;
import gps.graph.NullEdgeVertex;
import gps.node.MinaWritableIterable.MessagesIterator;
import gps.writable.IntSetWritable;
import gps.writable.IntegerIntegerMapWritable;
import gps.writable.IntWritable;

/**
 * Base class of the vertex classes of different SCC algoritms.
 *
 * @author semihsalihoglu
 */
public abstract class SCCBaseVertex extends NullEdgeVertex<SCCVertexValue, SCCMessageValue> {

	public SCCOptions options;
	protected List<Integer> tmpIntList;
	protected static SCCVertexValue value;
	public int latestIterationStartSuperstepNo;
	protected static int superstepGlobalMapsAreFrom = -1;
	protected static Map<Integer, Integer> numVerticesInFoundComponents = null;
	protected static Map<Integer, NodeWritable> graph;
	protected static Map<Integer, Integer> serialComputationResults = null;
	protected static Map<Integer, Integer> singletonResults = null;
	protected static int notTraversedEdgesForFBTS = 0;
	protected static Phase phase;
	protected boolean isColoring;
	protected Set<Integer> neighborIdsSet;
	protected boolean countNotTraversedEdges = false;
	protected boolean countActiveColorSets = false;
	protected Set<Integer> previousActiveColorSets = null;
	protected Set<Integer> activeColorSets = null;
	private boolean dumpFwRestVertices;
	
	public SCCBaseVertex(CommandLine commandLine) {
		options = new SCCOptions();
		options.parseOtherOpts(commandLine);
		tmpIntList = new ArrayList<Integer>();
		latestIterationStartSuperstepNo = -1;
		neighborIdsSet = new HashSet<Integer>();
	}
	
	@Override
	public void compute(Iterable<SCCMessageValue> messageValues, int superstepNo) {
		value = getValue();
		if (superstepNo <= 2) {
			doReverseEdgeComputation(messageValues, superstepNo);
			return;
		}
		if (superstepGlobalMapsAreFrom < superstepNo) {
			initializeMaps(superstepNo);
			superstepGlobalMapsAreFrom = superstepNo;
		}
		if (isFoundComponentOrSingleton()) {
			return;
		}

		switch (phase) {
		case FW_1:
			doFW1Computation(superstepNo);
			break;
		case FW_REST:
			doFWRestComputation(messageValues);
			break;
		case BW_1:
			if (value.colorID < 0) {
				return;
			}
			doBW1Computation();
			break;
		case BW_REST:
			if (value == null) {
				System.out.println("value is null!!!! id: " + getId());
			}
			if (value.colorID < 0) {
				return;
			}
			doBWRestComputation(messageValues, true /* send messages */);
			break;
		case BW_TRAVERSAL_GRAPH_FORMATION:
			if (value.bwId < 0 && value.colorID >= 0 && previousActiveColorSets.contains(value.colorID)) {
				SCCVertexValue copy = value.copy();
				doBWRestComputation(messageValues, false /* do not send messages */);
				copy.bwId = value.bwId;
				copy.type = value.type;
				graph.put(getId(), new NodeWritable(copy, new int[0]));
			}
			break;
		case BW_SERIAL_TRAVERSAL_RESULT_FINDING:
			// If the bw traversal is done serially and this vertex has found its result, then
			// it's fw id must equal it's bw id.
			if (serialComputationResults.containsKey(getId())) {
				System.out.println("vertexId: " + getId() + " has found its component serially.");
				setComponentFoundAndSendBWMessage(false /* do not send messages */);
			}
			break;
		case FINISHING_GRAPH_FORMATION:
			graph.put(getId(), new NodeWritable(getValue(), getNeighborIds()));
			break;
		case FINISHING_SERIAL_COMPUTATION_RESULT_FINDING:
			Integer singletonId = singletonResults.get(getId());
			if (singletonId != null) {
				setSingleton();
			} else {
				value.colorID = serialComputationResults.get(getId());
				setComponentFoundAndSendBWMessage(false /* do not send messages */);
			}
			break;
		case EDGE_CLEANING_1:
			sendMessages(getNeighborIds(), isColoring ?
				SCCMessageValue.newIntEdgeCleaningMessage(getId(), value.colorID) :
					SCCMessageValue.newByteEdgeCleaningMessage(getId(), (byte) value.colorID));
			break;
		case EDGE_CLEANING_2:
			tmpIntList.clear();
			for (SCCMessageValue messageValue : messageValues) {
				if (isColoring) {
					if (value.colorID == messageValue.int2) {
						tmpIntList.add(messageValue.int1);						
					}
				} else {
					if (value.colorID == messageValue.byte1) {
						tmpIntList.add(messageValue.int1);						
					}					
				}
			}
			value.transposeNeighbors = new int[tmpIntList.size()];
			for (int i = 0; i < tmpIntList.size(); ++i) {
				value.transposeNeighbors[i] = tmpIntList.get(i);
			}
			if (value.transposeNeighbors.length > 0) {
				sendMessages(value.transposeNeighbors,
					SCCMessageValue.newReverseGraphFormationMessage(getId()));
			}
			break;
		case EDGE_CLEANING_3_AND_TRIMMING:
			removeEdges();
			int[] neighborIdsToAdd = new int[
			    ((MessagesIterator<SCCMessageValue>) messageValues.iterator()).numWritableBytes / 5];
			int index = 0;
			for (SCCMessageValue messageValue : messageValues) {
				neighborIdsToAdd[index++] = messageValue.int1;
			}
			addEdges(neighborIdsToAdd, null);
			checkAndSetSingleton(); 
			if (value.type != SCCVertexType.SINGLETON) {
				value.colorID = -1;
				value.bwId = -1;
				value.type = SCCVertexType.NON_ROOT;
			}
			break;
		default:
			doFurtherComputation(messageValues, superstepNo, phase);
		}
	}

	private void doFW1Computation(int superstepNo) {
		checkAndSetSingleton();
		if (value.type == SCCVertexType.SINGLETON) {
			return;
		}
		initializeFwIdBwIdAndType(value);
		latestIterationStartSuperstepNo = superstepNo;
		setFWIdAndSendFWTraversalMessages(getId());
	}

	private void checkAndSetSingleton() {
		if (getNeighborsSize() == 0 || value.transposeNeighbors.length == 0) {
			setSingleton();
		}
	}

	private void setSingleton() {
		// -2 indicates that this vertex is a scc of its own.
		value.colorID = -2; // latestIterationStartSuperstepNo
		value.bwId = -2;
		value.type = SCCVertexType.SINGLETON;
		value.transposeNeighbors = new int[0];
		removeEdges();
	}

	protected void doFurtherComputation(Iterable<SCCMessageValue> messageValues, int superstepNo,
		Phase computationStage) {
		System.err.println("ERROR!!! Unkown computation stage: " + computationStage.name());
	}

	private void doBWRestComputation(Iterable<SCCMessageValue> messageValues, boolean sendMessages) {
		if (value.bwId >= 0) {
			return;
		}
		for (SCCMessageValue messageValue : messageValues) {
			if (isColoring) {
				if (value.colorID >= 0 && (value.colorID == messageValue.int1)) {
					setComponentFoundAndSendBWMessage(sendMessages);
					return;
				}
			} else {
				if (value.colorID >= 0 && (value.colorID == messageValue.byte1)) {
					setComponentFoundAndSendBWMessage(sendMessages);
					return;
				}
			}
		}
		if (countNotTraversedEdges && previousActiveColorSets.contains(value.colorID)) {
			getGlobalObjectsMap().putOrUpdateGlobalObject(SCCOptions.GOBJ_NOT_TRAVERSED_EDGES,
				new LongSumGlobalObject((long) value.transposeNeighbors.length));
		}
	}

	private void doBW1Computation() {
		if (isColoring) {
			if (value.colorID >= 0 && (value.colorID == getId())) {
				System.out.println("Vertex id: " + getId() + " is ROOT");
				setComponentFoundAndSendBWMessage(true /* send messages */);
			}
		} else {
			if (value.colorID >= 0 && (value.colorID == value.bwId)) {
				setComponentFoundAndSendBWMessage(true /* send messages */);
			}
		}
	}

	private void doFWRestComputation(Iterable<SCCMessageValue> messageValues) {
		int maxRootId = value.colorID;
		for (SCCMessageValue messageValue : messageValues) {
			if (isColoring) {
				if (messageValue.int1 > maxRootId) {
					maxRootId = messageValue.int1;
				}
			} else {
				if (messageValue.byte1 > maxRootId) {
					maxRootId = messageValue.byte1;
				}
			}
		}
		if (maxRootId > value.colorID) {
			setFWIdAndSendFWTraversalMessages(maxRootId);
		}
	}

	protected void setFWIdAndSendFWTraversalMessages(int maxRootId) {
		value.colorID = maxRootId;
		if (dumpFwRestVertices) {
			System.out.println("maxRootId: " + maxRootId);
		}
		if (!isColoring || getNeighborsSize() > 0) {
			// NOTE: When running SinglePivot, it is critical that we add the global object even if the
			// neighborsize is 0, because that indicates to us that there were at least some roots picked and
			// some sccs found. Otherwise, when no roots are picked, we terminate the computation.
			if (value.type != SCCVertexType.FOUND_COMPONENT) {
				getGlobalObjectsMap().putOrUpdateGlobalObject(SCCOptions.GOBJ_NUM_NOTIFIED,
					new LongSumGlobalObject((long) getNeighborsSize()));
			}
			sendMessages(getNeighborIds(), isColoring ? SCCMessageValue.newIntTraversalIdMessage(maxRootId) :
				SCCMessageValue.newByteTraversalMessage((byte) maxRootId));
		}
	}

	private void setComponentFoundAndSendBWMessage(boolean sendBWMessage) {
		if (!isColoring) {
			value.bwId = (byte) value.colorID;
			value.colorID = latestIterationStartSuperstepNo;
		} else {
			value.bwId = 1;
		}
		value.type = SCCVertexType.FOUND_COMPONENT;
		if (countActiveColorSets) {
			if (!isColoring) {
				activeColorSets.add((int) value.bwId);
			} else {
				activeColorSets.add(value.colorID);
			}
		}
		if (sendBWMessage && value.transposeNeighbors.length > 0) {
			getGlobalObjectsMap().putOrUpdateGlobalObject(SCCOptions.GOBJ_NUM_NOTIFIED,
				new LongSumGlobalObject((long) value.transposeNeighbors.length));
			sendMessages(value.transposeNeighbors,
				isColoring ? SCCMessageValue.newIntTraversalIdMessage(value.colorID)
					: SCCMessageValue.newByteTraversalMessage(value.bwId));
		}
		value.transposeNeighbors = new int[0];
		removeEdges();
	}

	protected void initializeMaps(int superstepNo) {
		phase = Phase.getComputationStageFromId(
			((IntWritable) getGlobalObjectsMap().getGlobalObject(
				SCCOptions.GOBJ_COMP_PHASE).getValue()).getValue());
		System.out.println("phase: " + phase);
		graph = new HashMap<Integer, NodeWritable>();
		getGlobalObjectsMap().putGlobalObject(SCCOptions.GOBJ_GRAPH, new GraphGObj(graph));
		numVerticesInFoundComponents = new HashMap<Integer, Integer>();
		getGlobalObjectsMap().putGlobalObject(SCCOptions.GOBJ_FOUND_COMPONENT_SIZES_MAP,
			new IntegerIntegerSumMapGObj(numVerticesInFoundComponents));
		if (getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_SERIAL_COMPUTATION_RESULTS) != null) {
			System.out.println("SerialComputationResults is not empty!!");
			serialComputationResults =
				((IntegerIntegerMapWritable) getGlobalObjectsMap().getGlobalObject(
					SCCOptions.GOBJ_SERIAL_COMPUTATION_RESULTS).getValue()).integerIntegerMap;
			System.out.println("serialComputationResults size: " + serialComputationResults.size());
		}
		if (getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_SINGLETON_RESULTS) != null) {
			System.out.println("Singleton Results is not empty!!");
			singletonResults =
				((IntegerIntegerMapWritable) getGlobalObjectsMap().getGlobalObject(
					SCCOptions.GOBJ_SINGLETON_RESULTS).getValue()).integerIntegerMap;
			System.out.println("singletonResults size: " + singletonResults.size());
		}
		// If the active color sets global object is set, that means the master is counting
		// the number of not traversed edges in the backward traversal.
		countActiveColorSets = false;
		if (getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_ACTIVE_COLOR_SETS) != null) {
			countActiveColorSets = true;
			activeColorSets = ((IntSetWritable) getGlobalObjectsMap().getGlobalObject(
				SCCOptions.GOBJ_ACTIVE_COLOR_SETS).getValue()).value;
		}
		countNotTraversedEdges = false;
		if (getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_PREVIOUS_ACTIVE_COLOR_SETS) != null) {
			previousActiveColorSets = ((IntSetWritable) getGlobalObjectsMap().getGlobalObject(
				SCCOptions.GOBJ_PREVIOUS_ACTIVE_COLOR_SETS).getValue()).value;
			countNotTraversedEdges = true;
			if (options.isDebug) {
				dumpPreviousActiveColorSets();
			}
			getGlobalObjectsMap().putOrUpdateGlobalObject(SCCOptions.GOBJ_NOT_TRAVERSED_EDGES,
				new LongSumGlobalObject((long) 0));
		}
		System.out.println("countActiveColorSets: " + countActiveColorSets);
		System.out.println("countNotTraversedEdges: " + countNotTraversedEdges);
		if (globalObjectsMap.getGlobalObject("dump") != null) {
			dumpFwRestVertices = true;
		}
		initializeMoreObjects();
	}

	private void dumpPreviousActiveColorSets() {
		System.out.println("Dumping previous active color sets. size: " + previousActiveColorSets.size());
		for (int previousActiveColorSet : previousActiveColorSets) {
			System.out.print(" " + previousActiveColorSet);
		}
		System.out.println("End of dumping previous active color sets.");
	}

	// Nothing to do by default
	protected void initializeMoreObjects() {}

	protected void doReverseEdgeComputation(Iterable<SCCMessageValue> messageValues, int superstepNo) {
		if (superstepNo == 1) {
			SCCVertexValue vertexValue = new SCCVertexValue();
			setValue(vertexValue);
			initializeFwIdBwIdAndType(vertexValue);
			sendMessages(getNeighborIds(), SCCMessageValue.newReverseGraphFormationMessage(getId()));
		} else if (superstepNo == 2) {
			int numNeighbors = ((MessagesIterator<SCCMessageValue>) messageValues.iterator()).numWritableBytes / 5;
			value.transposeNeighbors = new int[numNeighbors];
			int neighborIndex = 0;
			for (SCCMessageValue messageValue : messageValues) {
				value.transposeNeighbors[neighborIndex] = messageValue.int1;
				neighborIndex++;
			}
			checkAndSetSingleton();
		}
	}

	private void initializeFwIdBwIdAndType(SCCVertexValue vertexValue) {
		vertexValue.colorID = -1;
		vertexValue.bwId = -1;
		if (vertexValue.type != SCCVertexType.FOUND_COMPONENT) {
			vertexValue.type = SCCVertexType.NON_ROOT;
		}
	}

	private boolean isFoundComponentOrSingleton() {
		if (value.type == SCCVertexType.FOUND_COMPONENT || value.type == SCCVertexType.SINGLETON) {
			getGlobalObjectsMap().putOrUpdateGlobalObject(SCCOptions.GOBJ_FOUND_COMPONENT_SIZES,
				new LongSumGlobalObject((long) 1));
			if (value.type == SCCVertexType.FOUND_COMPONENT) {
				if (options.isDebug && value.colorID >= 0) {
					int key = value.colorID;
					if (numVerticesInFoundComponents.containsKey(key)) {
						numVerticesInFoundComponents.put(key, numVerticesInFoundComponents.get(key) + 1);
					} else {
						numVerticesInFoundComponents.put(key, 1);
					}
				}
			}
			return true;
		}
		return false;
	}
}