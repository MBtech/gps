package gps.examples.maksclustering;

import org.apache.commons.cli.CommandLine;

import gps.globalobjects.GlobalObject;
import gps.globalobjects.DoubleSumGlobalObject;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.Master;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.writable.DoubleWritable;
import gps.writable.MinaWritable;

public class MaksClustering {
	private static final String SUM_OF_LOCAL_VALUES_SQUARED_BV_KEY = "sum-of-local-values-squared";
	private static final String SUM_OF_LOCAL_VALUES_BV_KEY = "sum-of-local-values";
	private static final String SUM_OF_ALL_VALUES_NORMALIZED_BV_KEY = "sum-of-all-values-normalized";
	private static final String NORM_OF_ALL_VALUES_BV_KEY = "norm-of-all-values";
	private static final String NUM_EDGES_CROSSING_PARTITION_BV_KEY = "num_edges_crossing_partition";
	private static final String NUM_VERTICES_IN_NEGATIVE_PARTITION_BV_KEY = "num_vertices_in_negative_partition";
	private static final String NUM_EDGES_IN_NEGATIVE_PARTITION_BV_KEY = "num_edges_in_negative_partition";
	private static final String NUM_VERTICES_IN_POSITIVE_PARTITION_BV_KEY = "num_vertices_in_positive_partition";
	private static final String NUM_EDGES_IN_POSITIVE_PARTITION_BV_KEY = "num_edges_in_positive_partition";

	public static class MaksClusteringVertex extends NullEdgeVertex<DoubleWritable, DoubleWritable> {

		public static int stopSuperstep = 100;

		@Override
		public void compute(Iterable<DoubleWritable> messageValues, int superstepNo) {
			if (superstepNo == (stopSuperstep + 2)) {
				int numEdgesCrossing = 0;
				for (DoubleWritable neighborValue : messageValues) {
					if (getValue().getValue() * neighborValue.getValue() < 0) {
						numEdgesCrossing++;
					}
				}
				getGlobalObjectsMap().putOrUpdateGlobalObject(
					NUM_EDGES_CROSSING_PARTITION_BV_KEY,
					new IntSumGlobalObject(numEdgesCrossing));
				if (getValue().getValue() < 0) {
					getGlobalObjectsMap().putOrUpdateGlobalObject(
						NUM_VERTICES_IN_NEGATIVE_PARTITION_BV_KEY,
						new IntSumGlobalObject(1));
					getGlobalObjectsMap().putOrUpdateGlobalObject(
						NUM_EDGES_IN_NEGATIVE_PARTITION_BV_KEY,
						new IntSumGlobalObject(getNeighborsSize()));
				} else {
					getGlobalObjectsMap().putOrUpdateGlobalObject(
						NUM_VERTICES_IN_POSITIVE_PARTITION_BV_KEY,
						new IntSumGlobalObject(1));
					getGlobalObjectsMap().putOrUpdateGlobalObject(
						NUM_EDGES_IN_POSITIVE_PARTITION_BV_KEY,
						new IntSumGlobalObject(getNeighborsSize()));
				}
				voteToHalt();
				return;
			}

			if (superstepNo == 1) {
				String printStr = "";
				for (int i : getNeighborIds()) {
					printStr += " " + i;
				}
				System.out.println("neighbors: " + printStr);
				sendMessages(getNeighborIds(), getValue());
				return;
			}
			int graphSize = getGraphSize();
			double currentState = getValue().getValue();

			if ((superstepNo % 2) == 0) {
				double sum = 0.0;
				String printStr = "";
				for (DoubleWritable message : messageValues) {
					sum += message.getValue();
					printStr += " " + message.getValue();
				}
				System.out.println("messages: " + printStr);

				System.out.println("Inside the if. sum: " + sum);
				currentState = (currentState * (2*graphSize - getNeighborsSize()))  + sum;
				getGlobalObjectsMap().putOrUpdateGlobalObject(SUM_OF_LOCAL_VALUES_SQUARED_BV_KEY,
					new DoubleSumGlobalObject(
						new DoubleWritable(currentState * currentState).getValue()));
				getGlobalObjectsMap().putOrUpdateGlobalObject(SUM_OF_LOCAL_VALUES_BV_KEY,
					new DoubleSumGlobalObject(new DoubleWritable(currentState).getValue()));
			} else {
				double norm =
					((DoubleWritable) getGlobalObjectsMap().getGlobalObject(
						NORM_OF_ALL_VALUES_BV_KEY).getValue()).getValue();
				double sumOfAllV2sNormalized = ((DoubleWritable) getGlobalObjectsMap()
					.getGlobalObject(SUM_OF_ALL_VALUES_NORMALIZED_BV_KEY).getValue()).getValue();
				System.out.println("Inside the else. norm: " + norm + " sumOfAllV2sNormalized: "
					+ sumOfAllV2sNormalized);
				currentState = currentState / norm;
				currentState = currentState - (sumOfAllV2sNormalized / graphSize);
				System.out.println("globalId: " + getId() + " graphSize: " + graphSize
					+ " neighborsSize: " + getNeighborsSize() + " currentState: " + currentState);
				sendMessages(getNeighborIds(), new DoubleWritable(currentState));
			}
			setValue(new DoubleWritable(currentState));
			
			System.out.println("Exiting compute(): newState: " + currentState);
		}

		@Override
		public DoubleWritable getInitialValue(int id) {
			return new DoubleWritable(Math.random() - 0.5);
		}
	}

	public static class MaksClusteringMaster extends Master {

		@Override
		public void compute(int superstepNo) {
			System.out.println("Inside MaksClustering master.compute()");
			GlobalObject<? extends MinaWritable> sumOfLocalValuesSquaredBV =
				globalObjectsMap.getGlobalObject(SUM_OF_LOCAL_VALUES_SQUARED_BV_KEY);
			if (sumOfLocalValuesSquaredBV != null) {
				double norm = ((DoubleWritable) sumOfLocalValuesSquaredBV.getValue()).getValue(); //Math.sqrt(
				globalObjectsMap.removeGlobalObject(SUM_OF_LOCAL_VALUES_SQUARED_BV_KEY);
				globalObjectsMap.putGlobalObject(NORM_OF_ALL_VALUES_BV_KEY,
					new DoubleSumGlobalObject(norm));
				GlobalObject<? extends MinaWritable> sumOfLocalValuesBV =
					globalObjectsMap.getGlobalObject(SUM_OF_LOCAL_VALUES_BV_KEY);
				double sumOfLocalValuesNormalized =
					((DoubleWritable) sumOfLocalValuesBV.getValue()).getValue();
				sumOfLocalValuesNormalized /= norm;
				globalObjectsMap.removeGlobalObject(SUM_OF_LOCAL_VALUES_BV_KEY);
				globalObjectsMap.putGlobalObject(SUM_OF_ALL_VALUES_NORMALIZED_BV_KEY,
					new DoubleSumGlobalObject(sumOfLocalValuesNormalized));
			}
			super.compute(superstepNo);
		}
	}

	public static class MaksClusteringVertexFactory extends NullEdgeVertexFactory<DoubleWritable, DoubleWritable> {

		@Override
		public NullEdgeVertex<DoubleWritable, DoubleWritable> newInstance(CommandLine commandLine) {
			return new MaksClusteringVertex();
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return MaksClusteringVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return MaksClusteringVertex.class;
		}

		@Override
		public Class<?> getVertexValueClass() {
			return DoubleWritable.class;
		}

		@Override
		public Class<?> getMessageValueClass() {
			return DoubleWritable.class;
		}
	}
}
