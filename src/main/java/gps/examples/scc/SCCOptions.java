package gps.examples.scc;

import gps.examples.scc.SCCComputationPhase.Phase;
import gps.node.GPSNodeRunner;

import org.apache.commons.cli.CommandLine;

/**
 * Contains flags to the vertex and master classes for different SCC algorithms.
 *
 * @author semihsalihoglu
 */
public class SCCOptions {

	public static final String GOBJ_NUM_SUPERNODES = "num_supernodes";
	public static final String GOBJ_COMP_PHASE = "phase";
	public static final String GOBJ_NUM_NOTIFIED = "num-notified";
	public static final String GOBJ_RANDOM_ROOT_PICKER = "rrp";
	public static final String GOBJ_RECURSION_SIZES = "rs";
	public static final String GOBJ_FOUND_COMPONENT_SIZES_MAP = "fcsm";
	public static final String GOBJ_GRAPH = "graph";
	public static final String GOBJ_NOT_TRAVERSED_EDGES = "nte";
	public static final String GOBJ_NUM_NOTCONVERGED_VERTICES_WITH_INACTIVE_COLOR_SETS = "nncvwics";
	// Stores the results of the master's serial computation size
	public static final String GOBJ_SERIAL_COMPUTATION_RESULTS = "scr";
	public static final String GOBJ_SINGLETON_RESULTS = "sr";
	public static final String GOBJ_FOUND_COMPONENT_SIZES = "fcs";
	public static final String GOBJ_ALGORITHM_TO_RUN = "atr";
	public static final String GOBJ_ACTIVE_COLOR_SETS = "active-cs";
	public static final String GOBJ_PREVIOUS_ACTIVE_COLOR_SETS = "pre-active-cs";
	
	public static final String DEBUG_RUN_FLAG = "debug";
	public static final String DUMP_COMPONENT_SIZES = "dumpComponentSizes";
	public static final String NUM_ROOTS_TO_PICK_PER_COMPONENT = "nrpc";
	public static final String TERMINATION_STAGE_FLAG = "tstage";
	public static final String MAX_SINGLE_PIVOT_ITERATIONS_TO_RUN_FLAG = "mspitr";
	public static final String MAX_SUPERSTEPS_TO_RUN_FLAG = "msstr";
	public static final String DO_EDGE_CLEANING_STEP_FLAG = "edgecleaning";
	public static final String FINISHING_BACKWARD_TRAVERSAL_SERIALLY = "fbts";
	public static final String FINDING_REMAININING_SCCS_SERIALLY = "frss";
	public static final String PERCENTAGE_OF_VERTICES_IN_GIANT_SCC = "pvigs";
	public static final String NUM_EDGES_FOR_SERIAL_COMPUTATION = "nefsc";
	public static final String DUMP_ALL_COMPONENTS = "dac";

	public boolean isDebug = false;
	public int numRootsToPickPerComponent = 1;
	public int maxSinglePivotIterationsToRun = 1;
	public boolean dumpComponentSizes = false;
	public Phase terminationStage = null;
	public int maxSuperstepsToRun = -1;
	public boolean doEdgeCleaning = false;
	public boolean doSerialBWTraversal = false;
	public boolean doSerialFinishingComputation = false;
	public int numEdgesForSerialComputation = 1000000;
	public boolean dumpAllComponents = false;
	public double percetageOfVerticesInGiantSCC;

	protected void parseOtherOpts(CommandLine commandLine) {
		String otherOptsStr = commandLine.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		System.out.println("otherOptsStr: " + otherOptsStr);
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length; ) {
				String flag = split[index++];
				String value = split[index++];
				if (DEBUG_RUN_FLAG.equals(flag)) {
					isDebug = Boolean.parseBoolean(value);
				} else if (NUM_ROOTS_TO_PICK_PER_COMPONENT.equals(flag)) {
					numRootsToPickPerComponent = Integer.parseInt(value);
				} else if (DUMP_COMPONENT_SIZES.equals(flag)) {
					dumpComponentSizes = "true".equals(value);
				} else if (TERMINATION_STAGE_FLAG.equals(flag)) {
					terminationStage = Phase.valueOf(value.toUpperCase());
				} else if (MAX_SINGLE_PIVOT_ITERATIONS_TO_RUN_FLAG.equals(flag)) {
					maxSinglePivotIterationsToRun = Integer.parseInt(value);
				} else if (MAX_SUPERSTEPS_TO_RUN_FLAG.equals(flag)) {
					maxSuperstepsToRun = Integer.parseInt(value);
				} else if (DO_EDGE_CLEANING_STEP_FLAG.equals(flag)) {
					doEdgeCleaning = Boolean.parseBoolean(value);
				} else if (FINISHING_BACKWARD_TRAVERSAL_SERIALLY.equals(flag)) {
					doSerialBWTraversal = Boolean.parseBoolean(value);
				} else if (FINDING_REMAININING_SCCS_SERIALLY.equals(flag)) {
					doSerialFinishingComputation = Boolean.parseBoolean(value);
				} else if (NUM_EDGES_FOR_SERIAL_COMPUTATION.equals(flag)) {
					numEdgesForSerialComputation = Integer.parseInt(value);
				} else if (DUMP_ALL_COMPONENTS.equals(flag)) {
					dumpAllComponents = Boolean.parseBoolean(value);
				} else if (PERCENTAGE_OF_VERTICES_IN_GIANT_SCC.equals(flag)) {
					percetageOfVerticesInGiantSCC = Double.parseDouble(value);
				}
			}
		}
	}
}