package gps.examples.extendedsssp;

import gps.examples.extendedsssp.ExtendedSSSPComputationStage.ComputationStage;
import gps.globalobjects.GlobalObject;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;
import gps.writable.IntWritable;
import gps.writable.MinaWritable;

import org.apache.commons.cli.CommandLine;

/**
 * Vertex class for the extended sssp algorithm which first computes the
 * shortest paths from a single source vertex to all vertices. It then computes
 * the average distance of all vertices that are connected to the source vertex.
 * 
 * @author semihsalihoglu
 */
public class ExtendedSSSPVertex extends NullEdgeVertex<IntWritable, IntWritable>{
	
	private GlobalObject<? extends MinaWritable> stageGlobalObject;
	private ComputationStage computationStage;
	
	private static int DEFAULT_ROOT_ID = 0;
	private int root;
	public ExtendedSSSPVertex(CommandLine line) {
		String otherOptsStr = line.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		System.out.println("otherOptsStr: " + otherOptsStr);
		root = DEFAULT_ROOT_ID;
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length; ) {
				String flag = split[index++];
				String value = split[index++];
				if ("-root".equals(flag)) {
					root = Integer.parseInt(value);
					System.out.println("sourceId: " + root);
				}
			}
		}
	}

	@Override
	public void compute(Iterable<IntWritable> messageValues, int superstepNo) {
		int previousDistance = getValue().getValue();
		stageGlobalObject =
			getGlobalObjectsMap().getGlobalObject("comp-stage");
		computationStage = ExtendedSSSPComputationStage.ComputationStage.getComputationStageFromId(
			((IntWritable) stageGlobalObject.getValue()).getValue());
		switch(computationStage) {
		case SSSP_FIRST_SUPERSTEP:
			if (previousDistance == Integer.MIN_VALUE) {
			} else {
				getGlobalObjectsMap().putOrUpdateGlobalObject("not-converged-vertices",
					new IntSumGlobalObject(1));
				sendMessages(getNeighborIds(), getValue());
			}
			break;
		case SSSP_LATER_SUPERSTEP:
			int minValue = previousDistance - 1;
			int messageValueInt;
			for (IntWritable messageValue : messageValues) {
				messageValueInt = messageValue.getValue();
				if (messageValueInt < minValue) {
					minValue = messageValueInt;
				}
			}
			int currentDistance = minValue + 1;
			if (currentDistance < previousDistance) {
				IntWritable newState = new IntWritable(currentDistance);
				setValue(newState);
				getGlobalObjectsMap().putOrUpdateGlobalObject("not-converged-vertices",
					new IntSumGlobalObject(1));
				sendMessages(getNeighborIds(), newState);
			}
			break;
		case AVERAGE_COMPUTATION_STAGE:
			// If the vertex has a positive distance, add 1 to "sum-distances" and "num-connected"
			// The master class will divide "sum-distances" by "num-connected" to compute the
			// average
			if (getValue().getValue() > 0) {
				getGlobalObjectsMap().putOrUpdateGlobalObject("sum-distances", new IntSumGlobalObject(1));
				getGlobalObjectsMap().putOrUpdateGlobalObject("num-connected", new IntSumGlobalObject(1));
			}
			break;
		}
	}

	@Override
	public IntWritable getInitialValue(int id) {
		return id == root ? new IntWritable(0) : new IntWritable(Integer.MIN_VALUE);
	}
	
	/**
	 * Factory class for {@link MatchingVertex}.
	 * 
	 * @author semihsalihoglu
	 */
	public static class ExtendedSSSPVertexFactory extends NullEdgeVertexFactory<IntWritable, IntWritable> {

		@Override
		public NullEdgeVertex<IntWritable, IntWritable> newInstance(CommandLine commandLine) {
			return new ExtendedSSSPVertex(commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return ExtendedSSSPVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return ExtendedSSSPVertex.class;
		}

		public Class<?> getMasterClass() {
			return ExtendedSSSPMaster.class;
		}

		@Override
		public Class<?> getVertexValueClass() {
			return IntWritable.class;
		}

		@Override
		public Class<?> getMessageValueClass() {
			return IntWritable.class;
		}
	}
}
