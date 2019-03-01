package gps.examples.wcc.hybrid;

import org.apache.commons.cli.CommandLine;

import gps.examples.wcc.hybrid.WCCComputationPhase.Phase;
import gps.globalobjects.IntOverwriteGlobalObject;
import gps.globalobjects.StreamingRootPickerGObj;
import gps.graph.Master;
import gps.writable.IntWritable;
import gps.writable.StreamingRootPickerValue;
import gps.writable.StreamingRootPickerWritable;

public class SinglePivotHybridWCCMaster extends Master {
	protected SinglePivotHybridWCCOptions options;
	
	public SinglePivotHybridWCCMaster(CommandLine commandLine) {
		options = new SinglePivotHybridWCCOptions();
		options.parseOtherOpts(commandLine);
	}
	
	@Override
	public void compute(int superstepNo) {
		System.out.println(getClass().getCanonicalName() + ".compute() called");
		if (superstepNo == 1) {
			clearGlobalObjectsAndSetPhase(Phase.ROOT_PICKING);
			return;
		}
		Phase previousComputationStage = Phase.getComputationStageFromId(
			((IntWritable) getGlobalObjectsMap().getGlobalObject(
				SinglePivotHybridWCCOptions.GOBJ_COMP_PHASE).getValue()).getValue());
		System.out.println("previous phase: " + previousComputationStage);
		switch(previousComputationStage) {
		case ROOT_PICKING:
			StreamingRootPickerValue rootPickerValue = ((StreamingRootPickerWritable)
				getGlobalObjectsMap().getGlobalObject(SinglePivotHybridWCCOptions.GOBJ_RANDOM_ROOT_PICKER).getValue()).rootPicker;
			System.out.println("rootPickerValue: " + rootPickerValue);
			if (rootPickerValue.numValuesInserted == 0) {
				System.out.println("terminating computation because no root has been picked.");
				terminateComputation();
			}
			clearGlobalObjectsAndSetPhase(Phase.ROOT_DISCOVERY_AND_SINGLE_LP_1);
			getGlobalObjectsMap().putGlobalObject(SinglePivotHybridWCCOptions.GOBJ_RANDOM_ROOT_PICKER,
				new StreamingRootPickerGObj(new StreamingRootPickerWritable(rootPickerValue)));
			return;
		case ROOT_DISCOVERY_AND_SINGLE_LP_1:
			clearGlobalObjectsAndSetPhase(Phase.SINGLE_LP_REST);
			return;
		case SINGLE_LP_REST:
			IntWritable numPropagatingVertices = (IntWritable) getGlobalObjectsMap().getGlobalObject(
				SinglePivotHybridWCCOptions.GOBJ_NUM_PROPAGATING_VERTICES).getValue();
			System.out.println("numPropagatingVertices: " + numPropagatingVertices.getValue());
			if (numPropagatingVertices.getValue() > 0) {
				clearGlobalObjectsAndSetPhase(Phase.SINGLE_LP_REST);				
			} else {
				clearGlobalObjectsAndSetPhase(Phase.MAX_LP_1);
			}
			return;
		case MAX_LP_1:
			clearGlobalObjectsAndSetPhase(Phase.MAX_LP_REST);
			return;
		case MAX_LP_REST:
			terminateIfNumActiveVerticesIsZero();
			return;
		default:
			System.err.println("Unknown computationStage: " + previousComputationStage.name());
			throw new UnsupportedOperationException("Computation stage: " + previousComputationStage +
				" is not supported in this master. " + getClass().getCanonicalName());
		}
	}

	protected void clearGlobalObjectsAndSetPhase(Phase computationStage) {
		getGlobalObjectsMap().clearNonDefaultObjects();
		getGlobalObjectsMap().putGlobalObject(SinglePivotHybridWCCOptions.GOBJ_COMP_PHASE,
			new IntOverwriteGlobalObject(computationStage.getId()));
	}
}
