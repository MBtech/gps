package gps.examples.testnodeandedgevalueparsing;

import org.apache.commons.cli.CommandLine;

import gps.graph.Edge;
import gps.graph.Vertex;
import gps.graph.VertexFactory;
import gps.node.GPSJobConfiguration;
import gps.writable.IntWritable;

/**
 * GPS implementation of PageRank algorithm.
 * 
 * @author semihsalihoglu
 */
public class TestNodeAndEdgeValueParsing extends Vertex<IntWritable, IntWritable, IntWritable> {

	public TestNodeAndEdgeValueParsing(CommandLine line) {
	}

	@Override
	public void compute(Iterable<IntWritable> incomingMessages, int superstepNo) {
		if (superstepNo == 1) {
			for (Edge<IntWritable> edge : getOutgoingEdges()) {
				sendMessage(edge.getNeighborId(), edge.getEdgeValue());
			}
		}
		if (superstepNo == 2) {
			int sumOfVertexAndNeighborsValues = getValue().getValue();
			for (IntWritable message : incomingMessages) {
				sumOfVertexAndNeighborsValues += message.getValue();
			}
			setValue(new IntWritable(sumOfVertexAndNeighborsValues));
			voteToHalt();
		}
	}

	/**
	 * Factory class for {@link TestNodeAndEdgeValueParsing}.
	 * 
	 * @author semihsalihoglu
	 */
	public static class TestNodeAndEdgeValueParsingVertexFactory extends VertexFactory<IntWritable,
	IntWritable, IntWritable> {

		@Override
		public Vertex<IntWritable, IntWritable, IntWritable> newInstance(CommandLine commandLine) {
			return new TestNodeAndEdgeValueParsing(commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return TestNodeAndEdgeValueParsingVertexFactory.class;
		}

		@Override
		public boolean hasVertexValuesInInput() {
			return true;
		}

		@Override
		public Class<?> getVertexClass() {
			return TestNodeAndEdgeValueParsing.class;
		}

		@Override
		public Class<?> getVertexValueClass() {
			return IntWritable.class;
		}

		@Override
		public Class<?> getEdgeValueClass() {
			return IntWritable.class;
		}

		@Override
		public Class<?> getMessageValueClass() {
			return IntWritable.class;
		}
	}
}
