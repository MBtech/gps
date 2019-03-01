package gps.examples.edgevaluesssp;

import org.apache.commons.cli.CommandLine;

import gps.graph.Edge;
import gps.graph.Vertex;
import gps.graph.VertexFactory;
import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;
import gps.writable.IntWritable;

public class EdgeValueSSSPVertex extends Vertex<IntWritable, IntWritable, IntWritable> {

	private static int DEFAULT_SOURCE_ID = 0;
	private int sourceId;
	public EdgeValueSSSPVertex() {
	}
	
	public EdgeValueSSSPVertex(CommandLine line) {
		String otherOptsStr = line.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		System.out.println("otherOptsStr: " + otherOptsStr);
		sourceId = DEFAULT_SOURCE_ID;
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length; ) {
				String flag = split[index++];
				String value = split[index++];
				if ("-root".equals(flag)) {
					sourceId = Integer.parseInt(value);
					System.out.println("sourceId: " + sourceId);
				}
			}
		}
	}

	@Override
	public void compute(Iterable<IntWritable> messageValues, int superstepNo) {
		int previousDistance = getValue().getValue();
		if (superstepNo == 1) {
			if (previousDistance == Integer.MAX_VALUE) {
				voteToHalt();
			} else {
				sendMessageToNeighbors();
			}
		} else {
			int minValue = previousDistance;
			int messageValueInt;
			for (IntWritable messageValue : messageValues) {
				messageValueInt = messageValue.getValue();
				if (messageValueInt < minValue) {
					minValue = messageValueInt;
				}
			}
			int currentDistance = minValue;
			if (currentDistance < previousDistance) {
				IntWritable newState = new IntWritable(currentDistance);
				setValue(newState);
				sendMessageToNeighbors();
			} else {
				voteToHalt();
			}
		}
	}

	private void sendMessageToNeighbors() {
		for (Edge<IntWritable> outgoingEdge : getOutgoingEdges()) {
			IntWritable messageValue = new IntWritable(getValue().getValue()
				+ outgoingEdge.getEdgeValue().getValue());
			sendMessage(outgoingEdge.getNeighborId(),
				messageValue);
//			System.out.println("Sending a message to neighborId: "
//				+ outgoingEdge.getNeighborId() + " messageValue: " + messageValue);
		}
	}

	@Override
	public IntWritable getInitialValue(int id) {
		return id == sourceId ? new IntWritable(0) : new IntWritable(Integer.MAX_VALUE);
	}

	/**
	 * Factory class for {@link EdgeValueSSSPVertex}.
	 * 
	 * @author semihsalihoglu
	 */
	public static class EdgeValueSSSPVertexFactory
		extends VertexFactory<IntWritable, IntWritable, IntWritable> {
 
		@Override
		public Vertex<IntWritable, IntWritable, IntWritable> newInstance(CommandLine commandLine) {
			return new EdgeValueSSSPVertex(commandLine);
		}
	}
	
	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return EdgeValueSSSPVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return EdgeValueSSSPVertex.class;
		}

		@Override
		public Class<?> getVertexValueClass() {
			return IntWritable.class;
		}

		public Class<?> getEdgeValueClass() {
			return IntWritable.class;
		}

		@Override
		public Class<?> getMessageValueClass() {
			return IntWritable.class;
		}
	}
}
