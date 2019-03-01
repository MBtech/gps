package gps.examples.scc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Map.Entry;

import gps.examples.scc.SCCComputationPhase.Phase;
import gps.examples.scc.SCCVertexValue.SCCVertexType;
import gps.examples.scc.gobj.GraphWritable;
import gps.examples.scc.gobj.NodeWritable;
import gps.examples.scc.serial.Node;
import gps.examples.scc.serial.SCCFinderRunner;
import gps.globalobjects.BooleanOverwriteGlobalObject;
import gps.globalobjects.GlobalObject;
import gps.globalobjects.GlobalObjectsMap;
import gps.globalobjects.IntegerIntegerSumMapGObj;
import gps.globalobjects.IntOverwriteGlobalObject;
import gps.globalobjects.IntegerSetGlobalObject;
import gps.globalobjects.StreamingRootPickerGObj;
import gps.graph.Master;
import gps.writable.IntSetWritable;
import gps.writable.IntWritable;
import gps.writable.IntegerIntegerMapWritable;
import gps.writable.LongWritable;
import gps.writable.MinaWritable;
import gps.writable.StreamingRootPickerValue;
import gps.writable.StreamingRootPickerWritable;

import org.apache.commons.cli.CommandLine;

/**
 * Base master class for the algorithms for Coloring and Single-Pivot Coloring.
 *
 * @author semihsalihoglu
 */
public abstract class SCCBaseMaster extends Master {

	protected SCCOptions options;
	private Map<Integer, Integer> singletonResults;
	private int numFwRestSupersteps;
	
	
	public SCCBaseMaster(CommandLine commandLine) {
		options = new SCCOptions();
		options.parseOtherOpts(commandLine);
	}

	@Override
	public void compute(int superstepNo) {
		System.out.println(getClass().getCanonicalName() + ".compute() called");
		if (options.maxSuperstepsToRun > 0 && superstepNo == options.maxSuperstepsToRun) {
			terminateComputation();
			return;
		}
		if (superstepNo <= 2) {
			return;
		} else if (superstepNo == 3) {
			clearGlobalObjectsAndSetPhase(getNextInitialComputationStage(superstepNo));
			return;
		}
		Map<Integer, Integer> numVerticesInFoundComponents = ((IntegerIntegerMapWritable) getGlobalObjectsMap()
			.getGlobalObject(SCCOptions.GOBJ_FOUND_COMPONENT_SIZES_MAP).getValue()).integerIntegerMap;
		if (options.isDebug) {
			sortAndDumpNumVerticesInComponents(numVerticesInFoundComponents, "numVerticesInFoundComponents");
		}
		dumpNumVerticesWithFoundComponents();
		Phase previousComputationStage = Phase.getComputationStageFromId(
			((IntWritable) getGlobalObjectsMap().getGlobalObject(
				SCCOptions.GOBJ_COMP_PHASE).getValue()).getValue());
		System.out.println("previousComputationStage: " + previousComputationStage);
		if (options.terminationStage != null && previousComputationStage == options.terminationStage) {
			System.out.println("Reached termination stage: " + options.terminationStage);
			terminateComputation();
		}
		switch(previousComputationStage) {
		case ROOT_PICKING:
			System.out.println("PRINTING RANDOMROOTPICKERMAP: ");
			StreamingRootPickerValue rootPickerValue = ((StreamingRootPickerWritable)
				getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_RANDOM_ROOT_PICKER).getValue()).rootPicker;
			System.out.println("rootPickerValue: " + rootPickerValue);
			if (rootPickerValue.numValuesInserted == 0) {
				terminateComputation();
			}
			clearGlobalObjectsAndSetPhase(Phase.ROOT_DISCOVERY_AND_FW_1);
			getGlobalObjectsMap().putGlobalObject(SCCOptions.GOBJ_RANDOM_ROOT_PICKER,
				new StreamingRootPickerGObj(new StreamingRootPickerWritable(rootPickerValue)));
			return;
		case ROOT_DISCOVERY_AND_FW_1:
			clearGlobalObjectsAndSetPhase(Phase.FW_REST);
			return;
		case FW_1:
			if (getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_NUM_NOTIFIED) != null) {
				System.out.println("numNotified: " + (getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_NUM_NOTIFIED).getValue()));
				numFwRestSupersteps = 0;
				clearGlobalObjectsAndSetPhase(Phase.FW_REST);
			} else {
				terminateComputation();
			}
			return;
		case FW_REST:
			if (getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_NUM_NOTIFIED) != null) {
				numFwRestSupersteps++;
				System.out.println("numNotified: "
					+ getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_NUM_NOTIFIED).getValue());
				clearGlobalObjectsAndSetPhase(Phase.FW_REST);
				System.out.println("numFwRestSupersteps: " + numFwRestSupersteps);
				if (numFwRestSupersteps >= 75) {
					getGlobalObjectsMap().putGlobalObject("dump", new BooleanOverwriteGlobalObject(true));
				}
			} else {
				clearGlobalObjectsAndSetPhase(Phase.BW_1);
			}
			return;
		case BW_1:
			handleBW1OrBWRest(superstepNo);
			return;
		case BW_REST:
			handleBW1OrBWRest(superstepNo);
			return;
		case BW_TRAVERSAL_GRAPH_FORMATION:
			Map<Integer, Integer> foundVertices = doSerialBWTraversal();
			clearGlobalObjectsAndSetPhase(Phase.BW_SERIAL_TRAVERSAL_RESULT_FINDING);
			globalObjectsMap.putGlobalObject(SCCOptions.GOBJ_SERIAL_COMPUTATION_RESULTS,
				new IntegerIntegerSumMapGObj(foundVertices));
			return;
		case BW_SERIAL_TRAVERSAL_RESULT_FINDING:
			moveToStageAfterBWTraversalStages(superstepNo);
			return;
		case EDGE_CLEANING_1:
			clearGlobalObjectsAndSetPhase(Phase.EDGE_CLEANING_2);
			return;
		case EDGE_CLEANING_2:
			clearGlobalObjectsAndSetPhase(Phase.EDGE_CLEANING_3_AND_TRIMMING);
			return;
		case EDGE_CLEANING_3_AND_TRIMMING:
			clearGlobalObjectsAndSetPhase(getNextInitialComputationStage(superstepNo));
			return;
		case FINISHING_GRAPH_FORMATION:
			foundVertices = doSerialFinishing();
			clearGlobalObjectsAndSetPhase(Phase.FINISHING_SERIAL_COMPUTATION_RESULT_FINDING);
			globalObjectsMap.putGlobalObject(SCCOptions.GOBJ_SERIAL_COMPUTATION_RESULTS,
				new IntegerIntegerSumMapGObj(foundVertices));
			globalObjectsMap.putGlobalObject(SCCOptions.GOBJ_SINGLETON_RESULTS,
				new IntegerIntegerSumMapGObj(singletonResults));
			return;
		case FINISHING_SERIAL_COMPUTATION_RESULT_FINDING:
			terminateComputation();
			return;
		default:
			System.err.println("Unknown computationStage: " + previousComputationStage.name());
			throw new UnsupportedOperationException("Computation stage: " + previousComputationStage +
				" is not supported in this master. " + getClass().getCanonicalName());
		}
	}

	private void dumpNumVerticesWithFoundComponents() {
		if (getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_FOUND_COMPONENT_SIZES) != null) {
			System.out.println("numVerticesWithFoundComponents: "
				+ getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_FOUND_COMPONENT_SIZES).getValue());
		}
	}

	private Map<Integer, Integer> doSerialFinishing() {
		Map<Integer, NodeWritable> graph = ((GraphWritable) getGlobalObjectsMap().getGlobalObject(
			SCCOptions.GOBJ_GRAPH).getValue()).graph;
		singletonResults = new HashMap<Integer, Integer>();
		SCCFinderRunner sccFinderRunner = new SCCFinderRunner();
		for (Entry<Integer, NodeWritable> entry : graph.entrySet()) {
			int sourceId = entry.getKey();
			for (int neighborId : entry.getValue().neighbors) {
				sccFinderRunner.addEdge(sourceId, neighborId);			
			}
		}
		sccFinderRunner.findSCCs();
		List<Set<Node>> sccs = sccFinderRunner.sccFinder.sccs;
		Map<Integer, Integer> foundVertices = new HashMap<Integer, Integer>();
		for (Set<Node> scc : sccs) {
			if (scc.size() == 1) {
				singletonResults.put(scc.iterator().next().id, 1);
			} else {
				int maxId = getMaxId(scc);
				for (Node node : scc) {
					foundVertices.put(node.id, maxId);
				}
			}
		}
		return foundVertices;
	}

	private int getMaxId(Set<Node> scc) {
		int maxId = -1;
		for (Node node : scc) {
			if (node.id > maxId) {
				maxId = node.id;
			}
		}
		return maxId;
	}

	private Map<Integer, Integer> doSerialBWTraversal() {
		Map<Integer, NodeWritable> graph = ((GraphWritable) getGlobalObjectsMap().getGlobalObject(
			SCCOptions.GOBJ_GRAPH).getValue()).graph;
		System.out.println("size of the graph: " + graph.size());
		Stack<Integer> stack = new Stack<Integer>();
		SCCVertexValue value;
		for (Entry<Integer, NodeWritable> vertex : graph.entrySet()) {
			value = vertex.getValue().value;
			if (SCCVertexType.FOUND_COMPONENT == value.type) {
				stack.push(vertex.getKey());
			}
		}
		int vertexId;
		SCCVertexValue neighborValue;
		Map<Integer, Integer> foundVertices = new HashMap<Integer, Integer>();
		long timeBefore = System.currentTimeMillis();
		System.out.println("Starting serial bw traversal. numVerticesInStack: " + stack.size());
		while (!stack.isEmpty()) {
			vertexId = stack.pop();
			value = graph.get(vertexId).value;
			for (int neighborId : value.transposeNeighbors) {
				if (graph.containsKey(neighborId)) {
					neighborValue = graph.get(neighborId).value;
					if (neighborValue.bwId < 0 && value.colorID == neighborValue.colorID) {
						neighborValue.bwId = 1;
						stack.push(neighborId);
						foundVertices.put(neighborId, 1);
					}
				}
			}
		}
		System.out.println("Done with serial bw traversal... TotalTime: " + (System.currentTimeMillis() - timeBefore));
		return foundVertices;
	}

	private void handleBW1OrBWRest(int superstepNo) {
		if (getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_NUM_NOTIFIED) != null) {
			long numNotified = ((LongWritable) getGlobalObjectsMap().getGlobalObject(
				SCCOptions.GOBJ_NUM_NOTIFIED).getValue()).getValue();
			System.out.println("numNotified: " + numNotified);
			if (!options.doSerialBWTraversal) {
				clearGlobalObjectsAndSetPhase(Phase.BW_REST);
			} else {
				boolean activeSetsCounted = getGlobalObjectsMap().getGlobalObject(
					SCCOptions.GOBJ_ACTIVE_COLOR_SETS) != null;
				System.out.println("activeSetsCounted: " + activeSetsCounted);
				if (!activeSetsCounted) {
					if (numNotified < options.numEdgesForSerialComputation) {
						System.out.println("active sets was not counted and numNotified is small enough that" +
							" we now count active sets...");
						clearAndSetPhaseToBWRestAndAddEmptyActiveColorSets();
					} else {
						System.out.println("active sets was not counted but numNotified is very large." +
							" just moving to bw_rest.");
						clearGlobalObjectsAndSetPhase(Phase.BW_REST);
					}
				} else {
					Long numNotTraversedEdges = null;
					if (getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_NOT_TRAVERSED_EDGES) != null) {
						numNotTraversedEdges = ((LongWritable) getGlobalObjectsMap().getGlobalObject(
							SCCOptions.GOBJ_NOT_TRAVERSED_EDGES).getValue()).getValue();
					}
					Long numNotConvergedVerticesWithInactiveColorSets = null;
					if (getGlobalObjectsMap().getGlobalObject(
						SCCOptions.GOBJ_NUM_NOTCONVERGED_VERTICES_WITH_INACTIVE_COLOR_SETS) != null) {
						numNotConvergedVerticesWithInactiveColorSets = ((LongWritable) getGlobalObjectsMap().getGlobalObject(
							SCCOptions.GOBJ_NUM_NOTCONVERGED_VERTICES_WITH_INACTIVE_COLOR_SETS).getValue()).getValue();
					}
					System.out.println("bw traversal numNotTraversedEdges: " + numNotTraversedEdges +
						" numNotConvergedVerticesWithInactiveColorSets: " + numNotConvergedVerticesWithInactiveColorSets);
					GlobalObject<? extends MinaWritable> previousActiveColorSets =
						getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_ACTIVE_COLOR_SETS);
					if (numNotTraversedEdges == null ||
						numNotTraversedEdges > options.numEdgesForSerialComputation) {
						System.out.println("Just maintaining the current active color sets by putting" +
							" into previous active sets.");
						clearAndSetPhaseToBWRestAndAddEmptyActiveColorSets();
						getGlobalObjectsMap().putGlobalObject(SCCOptions.GOBJ_PREVIOUS_ACTIVE_COLOR_SETS,
							previousActiveColorSets);
					} else {
						System.out.println("Setting phase to BW_TRAVERSAL_GRAPH_FORMATION!!!");
						clearGlobalObjectsAndSetPhase(Phase.BW_TRAVERSAL_GRAPH_FORMATION);
						getGlobalObjectsMap().putGlobalObject(SCCOptions.GOBJ_PREVIOUS_ACTIVE_COLOR_SETS,
							previousActiveColorSets);
					}
				}
			}
		} else {
			moveToStageAfterBWTraversalStages(superstepNo);
		}
	}

	private void clearAndSetPhaseToBWRestAndAddEmptyActiveColorSets() {
		clearGlobalObjectsAndSetPhase(Phase.BW_REST);
		getGlobalObjectsMap().putGlobalObject(SCCOptions.GOBJ_ACTIVE_COLOR_SETS,
			new IntegerSetGlobalObject());
	}

	private void moveToStageAfterBWTraversalStages(int superstepNo) {
		int numEdgesRemaining = -1;
		if (getGlobalObjectsMap().getGlobalObject(GlobalObjectsMap.NUM_EDGES) != null) {
			numEdgesRemaining = ((IntWritable) getGlobalObjectsMap().getGlobalObject(
				GlobalObjectsMap.NUM_EDGES).getValue()).getValue();
			System.out.println("numEdgesRemaining: " + numEdgesRemaining);
		}
		if (options.doSerialFinishingComputation && numEdgesRemaining < options.numEdgesForSerialComputation) {
			System.out.println("Starting to finish computation serially.");
			clearGlobalObjectsAndSetPhase(Phase.FINISHING_GRAPH_FORMATION);
		} else if (options.doEdgeCleaning) {
			System.out.println("DOING EDGE CLEANING!!!");
			clearGlobalObjectsAndSetPhase(Phase.EDGE_CLEANING_1);
		} else {
			System.out.println("NOT DOING EDGE CLEANING!!!");
			clearGlobalObjectsAndSetPhase(getNextInitialComputationStage(superstepNo));
		}
	}

	protected void clearGlobalObjectsAndSetPhase(Phase computationStage) {
		getGlobalObjectsMap().clearNonDefaultObjects();
		getGlobalObjectsMap().putGlobalObject(SCCOptions.GOBJ_COMP_PHASE,
			new IntOverwriteGlobalObject(computationStage.getId()));
		addAdditionalGlobalObjects();
	}

	private void sortAndDumpNumVerticesInComponents(Map<Integer, Integer> mapToDump, String mapName) {
		List<Entry<Integer, Integer>> componentIdSizeList = new ArrayList<Map.Entry<Integer,Integer>>(
			mapToDump.entrySet());
		Collections.sort(componentIdSizeList, new ComponentSizeComparator());
		System.out.println("DUMPING 10 items from " + mapName + ". totalNumItems: "
			+ componentIdSizeList.size());
		int numCounted = 0;
		for (Entry<Integer, Integer> componentIdSizePair : componentIdSizeList) {
			numCounted++;
			System.out.println("componentId: " + componentIdSizePair.getKey() + " size: "
				+ componentIdSizePair.getValue());
			if (!options.dumpAllComponents && numCounted >= 10) {
				break;
			}
		}
		System.out.println("END OF DUMPING " + mapName + "....");
	}

	private static class ComponentSizeComparator implements Comparator<Entry<Integer, Integer>> {

		@Override
		public int compare(Entry<Integer, Integer> component1, Entry<Integer, Integer> component2) {
			if (component1.getValue() > component2.getValue()) {
				return -1;
			} else if (component1.getValue() < component2.getValue()) {
				return 1;
			} else {
				return 0;
			}
		}
	}
	protected void addAdditionalGlobalObjects() { /* Nothing to do by default */}

	public abstract Phase getNextInitialComputationStage(int superstepNo);
}