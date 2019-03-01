package gps.examples.wcc.hybrid;

import org.apache.commons.cli.CommandLine;

import gps.examples.wcc.hybrid.WCCComputationPhase.Phase;
import gps.globalobjects.IntSumGlobalObject;
import gps.globalobjects.StreamingRootPickerGObj;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.writable.IntWritable;
import gps.writable.StreamingRootPickerValue;
import gps.writable.StreamingRootPickerWritable;

public class SingplePivotHybridWCCVertex extends NullEdgeVertex<IntWritable, IntWritable>{

	private int superstepGlobalMapsAreFrom = -1;
	private  StreamingRootPickerValue rootPickerValue;
	private IntWritable numPropagatingVertices = null;
	private int minValue;
	private Phase phase;

	public SingplePivotHybridWCCVertex(CommandLine commandline) {
		// Nothing to do.
	}


	@Override
	public void compute(Iterable<IntWritable> messageValues, int superstepNo) {
		if (superstepGlobalMapsAreFrom < superstepNo) {
			initializeGlobalVariables(superstepNo);
			superstepGlobalMapsAreFrom = superstepNo;
		}
		if (superstepNo == 1 && getNeighborsSize() == 0) {
			voteToHalt();
			return;
		}
		if (Integer.MAX_VALUE == getValue().value) {
			voteToHalt();
			return;
		}
		switch (phase) {
		case ROOT_PICKING:
			rootPickerValue.insertNewIntValue(getId());
			break;
		case ROOT_DISCOVERY_AND_SINGLE_LP_1:
			doRootDiscoveryAndSingleLP1Computation();
			break;
		case SINGLE_LP_REST:
			if (messageValues.iterator().hasNext()) {
				labelWithMaxIntValueSendMessageToNeighborsAndRemoveEdges();
				numPropagatingVertices.value++;
			}
			break;
		case MAX_LP_1:
			setValue(new IntWritable(getId()));
			sendMessages(getNeighborIds(), getValue());
			break;
		case MAX_LP_REST:
			minValue = getValue().getValue();
			for (IntWritable message : messageValues) {
				if (message.getValue() < minValue) {
					minValue = message.getValue();
				}
			}
			if (minValue < getValue().getValue()) {
				setValue(new IntWritable(minValue));
				sendMessages(getNeighborIds(), getValue());
			} else {
				voteToHalt();
			}
			break;
		}
	}

	@Override
	public IntWritable getInitialValue(int id) {
		return new IntWritable(-1);
	}
	
	private void doRootDiscoveryAndSingleLP1Computation() {
		byte rootIndex = rootPickerValue.getValueIndex(getId());
		if (rootIndex >= 0) {
			labelWithMaxIntValueSendMessageToNeighborsAndRemoveEdges();
		}
	}


	private void labelWithMaxIntValueSendMessageToNeighborsAndRemoveEdges() {
		setValue(new IntWritable(Integer.MAX_VALUE));
		sendMessages(getNeighborIds(), new IntWritable(Integer.MAX_VALUE));
		voteToHalt();
	}

	private void initializeGlobalVariables(int superstepNo) {
		phase = Phase.getComputationStageFromId(
			((IntWritable) getGlobalObjectsMap().getGlobalObject(
				SinglePivotHybridWCCOptions.GOBJ_COMP_PHASE).getValue()).getValue());
		StreamingRootPickerGObj streamingRootPickerGObj = (StreamingRootPickerGObj)
			getGlobalObjectsMap().getGlobalObject(SinglePivotHybridWCCOptions.GOBJ_RANDOM_ROOT_PICKER);
		if (streamingRootPickerGObj != null) {
			rootPickerValue = streamingRootPickerGObj.getValue().rootPicker;			
		} else {
			rootPickerValue = new StreamingRootPickerValue(1);
			getGlobalObjectsMap().putGlobalObject(SinglePivotHybridWCCOptions.GOBJ_RANDOM_ROOT_PICKER,
				new StreamingRootPickerGObj(new StreamingRootPickerWritable(rootPickerValue)));
		}
		numPropagatingVertices = new IntWritable(0);
		IntSumGlobalObject numNotConvergedVerticesGO = new IntSumGlobalObject();
		numNotConvergedVerticesGO.setValue(numPropagatingVertices);
		getGlobalObjectsMap().putGlobalObject(SinglePivotHybridWCCOptions.GOBJ_NUM_PROPAGATING_VERTICES,
			numNotConvergedVerticesGO);
	}
	
	public static class SingplePivotHybridWCCVertexFactory extends
		NullEdgeVertexFactory<IntWritable, IntWritable> {

		@Override
		public NullEdgeVertex<IntWritable, IntWritable> newInstance(CommandLine commandline) {
			return new SingplePivotHybridWCCVertex(commandline);
		}
	}
}
