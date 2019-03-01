package gps.examples.scc.singlepivot;

import org.apache.commons.cli.CommandLine;

import gps.examples.scc.SCCBaseVertex;
import gps.examples.scc.SCCVertexValue;
import gps.examples.scc.SCCMessageValue;
import gps.examples.scc.SCCOptions;
import gps.examples.scc.SCCVertexValue.SCCVertexType;
import gps.examples.scc.SCCComputationPhase.Phase;
import gps.globalobjects.StreamingRootPickerGObj;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.writable.StreamingRootPickerValue;
import gps.writable.StreamingRootPickerWritable;

public class SinglePivotVertex extends SCCBaseVertex {

	public SinglePivotVertex(CommandLine commandLine) {
		super(commandLine);
		this.isColoring = false;
	}

	// TODO(semih): Don't forget to initialize
	public static StreamingRootPickerValue nextRootPickerValue;
	public static StreamingRootPickerValue previousRootPickerValue;

	@Override
	protected void doFurtherComputation(Iterable<SCCMessageValue> messageValues, int superstepNo,
		Phase computationStage) {
		switch (computationStage) {
		case ROOT_PICKING:
			latestIterationStartSuperstepNo = superstepNo;
			nextRootPickerValue.insertNewIntValue(getId());
			break;
		case ROOT_DISCOVERY_AND_FW_1:
			doRootDiscoveryAndFW1Computation();
			break;
		default:
			System.out.println("Unknown computationStage: " + computationStage.name());
		}
	}

	private void doRootDiscoveryAndFW1Computation() {
		byte rootIndex = previousRootPickerValue.getValueIndex(getId());
		if (rootIndex >= 0) {
			System.out.println("ROOT is: " + getId() + " rootIndex: " + rootIndex);
			SCCVertexValue vertexValue = getValue();
			vertexValue.type = SCCVertexType.ROOT;
			// WARNING: We use the bwId as the root index
			vertexValue.bwId = rootIndex;
			setFWIdAndSendFWTraversalMessages(rootIndex);
		}
	}

	protected void initializeMoreObjects() {
		StreamingRootPickerGObj streamingRootPickerGObj = (StreamingRootPickerGObj)
			getGlobalObjectsMap().getGlobalObject(SCCOptions.GOBJ_RANDOM_ROOT_PICKER);
		if (streamingRootPickerGObj != null) {
			previousRootPickerValue = streamingRootPickerGObj.getValue().rootPicker;			
		}
		nextRootPickerValue = new StreamingRootPickerValue(options.numRootsToPickPerComponent);
		getGlobalObjectsMap().putGlobalObject(SCCOptions.GOBJ_RANDOM_ROOT_PICKER,
			new StreamingRootPickerGObj(new StreamingRootPickerWritable(nextRootPickerValue)));
	}

	public static class VertexFactory extends
		NullEdgeVertexFactory<SCCVertexValue, SCCMessageValue> {

		@Override
		public NullEdgeVertex<SCCVertexValue, SCCMessageValue> newInstance(CommandLine commandLine) {
			return new SinglePivotVertex(commandLine);
		}
	}
}