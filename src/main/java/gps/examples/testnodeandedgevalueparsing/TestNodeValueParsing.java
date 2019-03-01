package gps.examples.testnodeandedgevalueparsing;

import org.apache.commons.cli.CommandLine;

import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.writable.DoubleWritable;

/**
 * GPS implementation of PageRank algorithm.
 * 
 * @author semihsalihoglu
 */
public class TestNodeValueParsing extends NullEdgeVertex<DoubleWritable, DoubleWritable> {

	public TestNodeValueParsing(CommandLine line) {
	}

	@Override
	public void compute(Iterable<DoubleWritable> incomingMessages, int superstepNo) {
		voteToHalt();
	}

	@Override
	public DoubleWritable getInitialValue(int id) {
		return null;
	}

	/**
	 * Factory class for {@link TestNodeValueParsing}.
	 * 
	 * @author semihsalihoglu
	 */
	public static class TestNodeValueParsingVertexFactory extends NullEdgeVertexFactory<DoubleWritable, DoubleWritable> {

		@Override
		public NullEdgeVertex<DoubleWritable, DoubleWritable> newInstance(CommandLine commandLine) {
			return new TestNodeValueParsing(commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return TestNodeValueParsingVertexFactory.class;
		}

		@Override
		public boolean hasVertexValuesInInput() {
			return true;
		}

		@Override
		public Class<?> getVertexClass() {
			return TestNodeValueParsing.class;
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
