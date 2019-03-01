package gps.examples.highestoutdegreecounter;

import org.apache.commons.cli.CommandLine;

import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;
import gps.writable.IntWritable;

public class HighestOutdegreeCounterVertex extends NullEdgeVertex<IntWritable, IntWritable> {

	private static int DEFAULT_NUM_VERTICES_TO_DUMP = 1000;
	private int numVerticesToDump;
	public HighestOutdegreeCounterVertex(CommandLine line) {
		String otherOptsStr = line.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		System.out.println("otherOptsStr: " + otherOptsStr);
		numVerticesToDump = DEFAULT_NUM_VERTICES_TO_DUMP;
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length; ) {
				String flag = split[index++];
				String value = split[index++];
				if ("-nv".equals(flag)) {
					numVerticesToDump = Integer.parseInt(value);
					System.out.println("numberOfVerticesToDump: " + numVerticesToDump);
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
				sendMessages(getNeighborIds(), getValue());
			}
		} else {
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
				sendMessages(getNeighborIds(), newState);
			} else {
				voteToHalt();
			}
		}
	}

	@Override
	public IntWritable getInitialValue(int id) {
		return new IntWritable(0);
	}

	/**
	 * Factory class for {@link HighestOutdegreeCounterVertex}.
	 * 
	 * @author semihsalihoglu
	 */
	public static class HighestOutdegreeCounterVertexFactory extends NullEdgeVertexFactory<IntWritable, IntWritable> {

		@Override
		public NullEdgeVertex<IntWritable, IntWritable> newInstance(CommandLine commandLine) {
			return new HighestOutdegreeCounterVertex(commandLine);
		}
	}
	
	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return HighestOutdegreeCounterVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return HighestOutdegreeCounterVertex.class;
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
