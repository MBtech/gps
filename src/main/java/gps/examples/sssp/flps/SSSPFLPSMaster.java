package gps.examples.sssp.flps;

import org.apache.commons.cli.CommandLine;

import gps.examples.sssp.flps.SSSPFLPSPhase.Phase;
import gps.globalobjects.IntOverwriteGlobalObject;
import gps.graph.Master;
import gps.writable.IntWritable;

public class SSSPFLPSMaster extends Master {

	private SSSPFLPSOptions options;
	public SSSPFLPSMaster(CommandLine commandLine) {
		options = new SSSPFLPSOptions();
		options.parseOtherOpts(commandLine);
	}

	@Override
	public void compute(int superstepNo) {
		System.out.println(getClass().getCanonicalName() + ".compute() called");
		if (superstepNo == 1) {
			clearGlobalObjectsAndSetPhase(Phase.REGULAR_SSSP);
			return;
		}
		Phase previousComputationStage = Phase.getComputationStageFromId(
			((IntWritable) getGlobalObjectsMap().getGlobalObject(
				SSSPFLPSOptions.GOBJ_COMP_PHASE).getValue()).getValue());
		System.out.println("previous phase: " + previousComputationStage);
		switch(previousComputationStage) {
		case REGULAR_SSSP:
			
			int numRecentlyUpdatedVertices = ((IntWritable) globalObjectsMap.getGlobalObject(
				SSSPFLPSOptions.GOBJ_NUM_RECENTLY_UPDATED_VERTICES).getValue()).value;
			System.out.println("numRecentlyUpdatedVertices: " + numRecentlyUpdatedVertices);
			if (numRecentlyUpdatedVertices == 0) {
				this.continueComputation = false;
				return;
			}
			int numPotentiallyActiveVertices = ((IntWritable) globalObjectsMap.getGlobalObject(
				SSSPFLPSOptions.GOBJ_NUM_POTENTIALLY_ACTIVE_VERTICES).getValue()).value;
			System.out.println("numPotentiallyActiveVertices: " + numPotentiallyActiveVertices);
			clearGlobalObjectsAndSetPhase(Phase.REGULAR_SSSP);
			return;
		case GRAPH_FORMATION:
			return;
		case SERIAL_RESULT_FINDING:
			this.continueComputation = false;
			return;
		default:
			System.err.println("Unknown computationStage: " + previousComputationStage.name());
			throw new UnsupportedOperationException("Computation stage: " + previousComputationStage +
				" is not supported in this master. " + getClass().getCanonicalName());
		}
	}

	protected void clearGlobalObjectsAndSetPhase(Phase computationStage) {
		getGlobalObjectsMap().clearNonDefaultObjects();
		getGlobalObjectsMap().putGlobalObject(SSSPFLPSOptions.GOBJ_COMP_PHASE,
			new IntOverwriteGlobalObject(computationStage.getId()));
	}
}
