package gps.examples.mst;

import org.apache.commons.cli.CommandLine;

import gps.examples.mst.MSTComputationStage.ComputationStage;
import gps.examples.mst.MSTOptions.EDGE_STORAGE_TECHNIQUE;
import gps.globalobjects.IntOverwriteGlobalObject;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.Master;
import gps.writable.IntWritable;

public abstract class MSTMaster extends Master {
	protected MSTOptions options;
	protected EDGE_STORAGE_TECHNIQUE edgeStorageTechnique;
	protected int numSupernodes = Integer.MAX_VALUE;
	protected int latestECODIteration = -1;

	public MSTMaster(CommandLine commandLine) {
		options = new MSTOptions();
		options.parseOtherOpts(commandLine);
	}

	@Override
	public void compute(int superstepNo) {
		System.out.println("Master.compute() called for class. " + this.getClass().getCanonicalName());
		if (superstepNo == 1) {
			clearGlobalObjectsAndSetComputationStage(ComputationStage.IMMEDIATE_MIN_EDGE_PICKING);
			return;
		}
		ComputationStage computationStage = ComputationStage.getComputationStageFromId(
			((IntWritable) getGlobalObjectsMap().getGlobalObject(
			MSTOptions.GOBJ_COMP_STAGE).getValue()).getValue());
		if (options.terminationStage != null && computationStage == options.terminationStage) {
			System.out.println("Reached termination stage: " + options.terminationStage);
			terminateComputation();
		}
		System.out.println("Previous compStage: " + computationStage.name());
		dumpStatisticalGlobalObjects();
		switch(computationStage) {
		case IMMEDIATE_MIN_EDGE_PICKING:
			// We count the number of vertices who were not root and notified their parents
			// of their minimum edges.
			IntSumGlobalObject notifiedParent =
				(IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject("notified_parent");
			if (notifiedParent == null) {
				clearGlobalObjectsAndSetComputationStage(ComputationStage.POINTER_JUMPING_QUESTION_1);
			} else {
				clearGlobalObjectsAndSetComputationStage(ComputationStage.IMMEDIATE_MIN_EDGE_PICKING);
			}
			break;
		case POINTER_JUMPING_QUESTION_1:
			doPointerJumpingQuestionStageComputation();
			break;
		case POINTER_JUMPING_QUESTION:
			doPointerJumpingQuestionStageComputation();
			break;
		case POINTER_JUMPING_ANSWER:
			clearGlobalObjectsAndSetComputationStage(ComputationStage.POINTER_JUMPING_QUESTION);
			break;
		default:
			// Do nothing
			doFurtherMasterComputation(computationStage);
		}
	}

	private void doPointerJumpingQuestionStageComputation() {
		if (!checkIfEveryoneReachedTheRootAndGoToEitherEdgeCleaningOrNotifyNewRootWithSubverticesStep()) {
			clearGlobalObjectsAndSetComputationStage(ComputationStage.POINTER_JUMPING_ANSWER);
		}
	}

	private void dumpStatisticalGlobalObjects() {
		dumpIntSumGlobalObjectIfExists(MSTOptions.GOBJ_ERROR_VERTICES);
		dumpIntSumGlobalObjectIfExists(MSTOptions.GOBJ_NUM_EDGES_CUSTOM_COUNTING);
		dumpIntSumGlobalObjectIfExists(MSTOptions.GOBJ_NUM_VERTICES_CUSTOM_COUNTING);
		dumpIntSumGlobalObjectIfExists(MSTOptions.GOBJ_NUM_SUPERNODES);
		dumpIntSumGlobalObjectIfExists(MSTOptions.GOBJ_NUM_EDGES_REMOVED_IN_EDGE_CLEANING_1);
		dumpIntSumGlobalObjectIfExists(MSTOptions.GOBJ_NUM_EDGES_REMOVED_IN_EDGE_CLEANING_2);
	}

	protected abstract void doFurtherMasterComputation(ComputationStage computationStage);

	protected boolean checkIfEveryoneReachedTheRootAndGoToEitherEdgeCleaningOrNotifyNewRootWithSubverticesStep() {
		IntSumGlobalObject numVerticesPointingAtNonRoots =
			(IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject(MSTOptions.GOBJ_POINTING_AT_NON_ROOT);
		if (numVerticesPointingAtNonRoots == null) {
			System.out.println("numVerticesPointingAtNonRoots: null");
			if (EDGE_STORAGE_TECHNIQUE.AT_SELF == edgeStorageTechnique) {
				clearGlobalObjectsAndSetComputationStage(
					ComputationStage.NOTIFY_NEW_ROOT_THAT_SELF_IS_A_SUBVERTEX);
			} else if (EDGE_STORAGE_TECHNIQUE.AT_THE_ROOT == edgeStorageTechnique) {
				clearGlobalObjectsAndSetComputationStage(ComputationStage.AT_ROOT_EDGE_CLEANING_1);
			}
			return true;
		} else {
			System.out.println("numVerticesPointingAtNonRoots: "
				+ numVerticesPointingAtNonRoots.getValue().getValue());
			return false;
		}
	}

	protected void clearGlobalObjectsAndSetComputationStage(ComputationStage compStage) {
		getGlobalObjectsMap().clearNonDefaultObjects();
		getGlobalObjectsMap().putGlobalObject(MSTOptions.GOBJ_COMP_STAGE,
			new IntOverwriteGlobalObject(compStage.getId()));
		System.out.println("Next compStage: " + compStage);
	}
}
