package gps.examples.mst;

import gps.examples.mst.MSTComputationStage.ComputationStage;
import gps.node.GPSNodeRunner;

import org.apache.commons.cli.CommandLine;

public class MSTOptions {

	public static final String DEBUG_RUN_FLAG = "debug";
	public static final String TERMINATION_STAGE_FLAG = "tstage";
	public static final String USE_HASH_MAP_IN_EDGE_CLEANING = "uhmiec";
	public static final String NUM_EDGES_THRESHOLD_FOR_USING_HASH_MAP_IN_EDGE_CLEANING = "netfhm";
	public static final String NUM_SUPERNODES_THRESHOLD_FOR_SWITCHING_TO_AT_ROOT = "nstfsear";
	public static final String EDGE_CLEANING_ON_DEMAND = "ecod";
	public static final String NUM_EDGE_CLEANING_ON_DEMAND_ITERATIONS = "necodi";

	// Global Objects
	public static final String GOBJ_NUM_SUPERNODES = "nsn";
	public static final String GOBJ_COMP_STAGE = "stage";
	public static final String GOBJ_ERROR_VERTICES = "error-vertices";
	public static final String GOBJ_POINTING_AT_NON_ROOT = "pointing-at-non-root";
	public static final String GOBJ_NUM_EDGES_REMOVED_IN_EDGE_CLEANING_1 = "nerec1";
	public static final String GOBJ_NUM_EDGES_REMOVED_IN_EDGE_CLEANING_2 = "nerec2";
	public static final String GOBJ_NUM_EDGES_CUSTOM_COUNTING = "ne";
	public static final String GOBJ_NUM_VERTICES_CUSTOM_COUNTING = "nv";
	public static final String GOBJ_NUM_EDGES_FOR_EDGE_STORAGE_AT_SELF_CUSTOM_COUNTING = "neess";

	protected boolean isDebug = true;
	protected ComputationStage terminationStage = null;
	public boolean useHashMapInEdgeCleaningForEdgesAtRoot = true;
	public int numEdgesThresholdForUsingHashMap = 500;
	public int numSupernodesThresholdForSwitchingToEdgesAtRoot = 10000;
	public boolean isECOD = false;
	public int numECODIterations = 1;
	protected boolean isFirstECODMinEdgePickingQuestion = true;
	
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
				} else if (TERMINATION_STAGE_FLAG.equals(flag)) {
					terminationStage = ComputationStage.valueOf(value.toUpperCase());
				} else if (USE_HASH_MAP_IN_EDGE_CLEANING.equals(flag)) {
					useHashMapInEdgeCleaningForEdgesAtRoot = Boolean.parseBoolean(value);
				} else if (NUM_EDGES_THRESHOLD_FOR_USING_HASH_MAP_IN_EDGE_CLEANING.equals(flag)) {
					numEdgesThresholdForUsingHashMap = Integer.parseInt(value);
				} else if (NUM_SUPERNODES_THRESHOLD_FOR_SWITCHING_TO_AT_ROOT.equals(flag)) {
					numSupernodesThresholdForSwitchingToEdgesAtRoot = Integer.parseInt(value);
				} else if (EDGE_CLEANING_ON_DEMAND.equals(flag)) {
					isECOD = Boolean.parseBoolean(value);
				} else if (NUM_EDGE_CLEANING_ON_DEMAND_ITERATIONS.equals(flag)) {
					numECODIterations = Integer.parseInt(value);
				}
			}
		}
		System.out.println("useHashMapInEdgeCleaningForEdgesAtRoot: " + useHashMapInEdgeCleaningForEdgesAtRoot);
		System.out.println("numEdgesThresholdForUsingHashMap: " + numEdgesThresholdForUsingHashMap);
	}

	public static enum EDGE_STORAGE_TECHNIQUE {
		AT_THE_ROOT,
		AT_SELF
	}
}