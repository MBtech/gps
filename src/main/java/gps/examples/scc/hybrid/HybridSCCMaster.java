package gps.examples.scc.hybrid;

import java.util.HashMap;
import java.util.Map;

import gps.examples.scc.SCCBaseMaster;
import gps.examples.scc.SCCOptions;
import gps.examples.scc.SCCComputationPhase.Phase;
import gps.globalobjects.GlobalObject;
import gps.globalobjects.IntOverwriteGlobalObject;
import gps.writable.LongWritable;
import gps.writable.MinaWritable;

import org.apache.commons.cli.CommandLine;

public class HybridSCCMaster extends SCCBaseMaster {

	private int numSinglePivotIterationsRan;
	private Algorithm currentAlgorithmRunning;

	public HybridSCCMaster(CommandLine commandLine) {
		super(commandLine);
		numSinglePivotIterationsRan = 0;
		currentAlgorithmRunning = Algorithm.DCSC;
	}

	@Override
	public Phase getNextInitialComputationStage(int superstepNo) {
		GlobalObject<? extends MinaWritable> foundComponentVerticesWritable = getGlobalObjectsMap().getGlobalObject(
			SCCOptions.GOBJ_FOUND_COMPONENT_SIZES);
		int foundComponentVertices = -1;
		if (foundComponentVerticesWritable != null) {
			foundComponentVertices = (int) ((LongWritable) foundComponentVerticesWritable.getValue()).getValue();
		}
		int numVerticesInGraph = getGraphSize();
		double percentageOfVerticesFound = (double) foundComponentVertices / (double) numVerticesInGraph;
		System.out.println("numVerticesInGraph: " + numVerticesInGraph);
		System.out.println("foundComponentVertices: " + foundComponentVertices);
		System.out.println("percentageOfVerticesFound: " + percentageOfVerticesFound);
		if ((currentAlgorithmRunning == Algorithm.DCSC) &&
			(percentageOfVerticesFound < options.percetageOfVerticesInGiantSCC) &&  (numSinglePivotIterationsRan < options.maxSinglePivotIterationsToRun)) {
			System.out.println("Continuing to run single-pivot...");
			numSinglePivotIterationsRan++;
			return Phase.ROOT_PICKING;			
		} else {
			System.out.println("Running coloring");
			currentAlgorithmRunning = Algorithm.COLORING;
			return Phase.FW_1;
		}
	}
	
	@Override
	protected void addAdditionalGlobalObjects() {
		getGlobalObjectsMap().putGlobalObject(SCCOptions.GOBJ_ALGORITHM_TO_RUN,
			 new IntOverwriteGlobalObject(currentAlgorithmRunning.getId()));
	}
	
	public static enum Algorithm {
		DCSC(0),
		COLORING(1);

		private static Map<Integer, Algorithm> idAlgorithmMap =
			new HashMap<Integer, Algorithm>();
		static {
			for (Algorithm type : Algorithm.values()) {
				idAlgorithmMap.put(type.id, type);
			}
		}

		private int id;

		private Algorithm(int id) {
			this.id = id;
		}

		public int getId() {
			return id;
		}

		public static Algorithm getAlgorithmFromId(int id) {
			return idAlgorithmMap.get(id);
		}
	}
}