package gps.examples.scc.hybrid;

import org.apache.commons.cli.CommandLine;

import gps.examples.scc.SCCOptions;
import gps.examples.scc.SCCVertexValue;
import gps.examples.scc.SCCMessageValue;
import gps.examples.scc.hybrid.HybridSCCMaster.Algorithm;
import gps.examples.scc.singlepivot.SinglePivotVertex;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.writable.IntWritable;

public class HybridSCCVertex extends SinglePivotVertex {

	public HybridSCCVertex(CommandLine commandLine) {
		super(commandLine);
	}

	public static class VertexFactory extends
		NullEdgeVertexFactory<SCCVertexValue, SCCMessageValue> {

		@Override
		public NullEdgeVertex<SCCVertexValue, SCCMessageValue> newInstance(CommandLine commandLine) {
			return new HybridSCCVertex(commandLine);
		}
	}

	protected void initializeMoreObjects() {
		super.initializeMoreObjects();
		isColoring = Algorithm.COLORING == Algorithm.getAlgorithmFromId(
			((IntWritable) getGlobalObjectsMap().getGlobalObject(
				SCCOptions.GOBJ_ALGORITHM_TO_RUN).getValue()).getValue());
		System.out.println("isColoring: " + isColoring);
	}
}