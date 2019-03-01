package gps.examples.sssp.flps;

import org.apache.commons.cli.CommandLine;

import gps.examples.sssp.SingleSourceAllVerticesShortestPathVertex;
import gps.examples.sssp.flps.SSSPFLPSPhase.Phase;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.writable.BooleanWritable;
import gps.writable.IntWritable;

public class SSSPFLPSVertex extends SingleSourceAllVerticesShortestPathVertex {
	private int superstepGlobalMapsAreFrom = -1;
	private Phase phase;
	private IntWritable numPotentiallyActiveVertices;
	private int value;
	
	public SSSPFLPSVertex(CommandLine line) {
		super(line);
		this.isFLPS = true;
	}

	@Override
	public void compute(Iterable<BooleanWritable> messageValues, int superstepNo) {
		if (superstepGlobalMapsAreFrom < superstepNo) {
			initializeGlobalVariables(superstepNo);
			superstepGlobalMapsAreFrom = superstepNo;
		}
		value = getValue().value;
		switch(phase) {
		case REGULAR_SSSP:
			if (value == Integer.MAX_VALUE && !messageValues.iterator().hasNext()) {
				numPotentiallyActiveVertices.value += getNeighborsSize();
				return;
			} else if (superstepNo > 1 && value < Integer.MAX_VALUE) {
				voteToHalt();
				return;
			} else {
				performRegularLabelPropagation(messageValues, superstepNo);
				return;
			}
		case GRAPH_FORMATION:
			break;
		case SERIAL_RESULT_FINDING:
			break;
		}
	}

	private void initializeGlobalVariables(int superstepNo) {
		phase = Phase.getComputationStageFromId(
			((IntWritable) getGlobalObjectsMap().getGlobalObject(
				SSSPFLPSOptions.GOBJ_COMP_PHASE).getValue()).getValue());
		numPotentiallyActiveVertices = new IntWritable(0);
		IntSumGlobalObject numPotentiallyActiveVerticesGO = new IntSumGlobalObject();
		numPotentiallyActiveVerticesGO.setValue(numPotentiallyActiveVertices);
		getGlobalObjectsMap().putGlobalObject(SSSPFLPSOptions.GOBJ_NUM_POTENTIALLY_ACTIVE_VERTICES,
			numPotentiallyActiveVerticesGO);
		
		numRecentlyUpdatedVertices = new IntWritable(0);
		IntSumGlobalObject numRecentlyUpdatedVerticesGO = new IntSumGlobalObject();
		numRecentlyUpdatedVerticesGO.setValue(numRecentlyUpdatedVertices);
		getGlobalObjectsMap().putGlobalObject(SSSPFLPSOptions.GOBJ_NUM_RECENTLY_UPDATED_VERTICES,
			numRecentlyUpdatedVerticesGO);
	}

	public static class SSSPFLPSVertexFactory extends
		NullEdgeVertexFactory<IntWritable, BooleanWritable> {
		@Override
		public NullEdgeVertex<IntWritable, BooleanWritable> newInstance(CommandLine commandline) {
			return new SSSPFLPSVertex(commandline);
		}
	}
}