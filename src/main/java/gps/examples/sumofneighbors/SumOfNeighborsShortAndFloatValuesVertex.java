package gps.examples.sumofneighbors;

import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.mina.core.buffer.IoBuffer;

import gps.examples.wcc.WeaklyConnectedComponentsVertex;
import gps.examples.wcc.WeaklyConnectedComponentsVertex.WeaklyConnectedComponentsVertexFactory;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.graph.VertexFactory;
import gps.node.GPSJobConfiguration;
import gps.writable.IntWritable;
import gps.writable.MinaWritable;

public class SumOfNeighborsShortAndFloatValuesVertex
	extends NullEdgeVertex<SumOfNeighborsShortAndFloatValuesVertex.TestWritable,
				   SumOfNeighborsShortAndFloatValuesVertex.TestWritable> {

	@Override
	public void compute(Iterable<TestWritable> messageValues, int superstepNo) {
		if (superstepNo == 1) {
			sendMessages(getNeighborIds(), getValue());
		} else {
			short shortSum = 0;
			float floatSum = 0;
			for (TestWritable messageValue : messageValues) {
				shortSum += messageValue.shortValue;
				floatSum += messageValue.floatValue;
			}
			setValue(new TestWritable(shortSum, floatSum));
			voteToHalt();
		}
	}

	@Override
	public TestWritable getInitialValue(int id) {
		Random random = new Random();
		return new TestWritable((short) random.nextInt(), random.nextFloat());
	}

	public static class SumOfNeighborsShortAndFloatValuesVertexFactory
		extends NullEdgeVertexFactory<SumOfNeighborsShortAndFloatValuesVertex.TestWritable,
		                      SumOfNeighborsShortAndFloatValuesVertex.TestWritable> {

		@Override
		public NullEdgeVertex<SumOfNeighborsShortAndFloatValuesVertex.TestWritable,
		              SumOfNeighborsShortAndFloatValuesVertex.TestWritable> newInstance(
		            	  CommandLine commandLine) {
			return new SumOfNeighborsShortAndFloatValuesVertex();
		}
	}

	public static class TestWritable extends MinaWritable {

		private short shortValue;
		private float floatValue;

		public TestWritable() {			
		}

		public TestWritable(short shortValue, float floatValue) {
			this.shortValue = shortValue;
			this.floatValue = floatValue;
		}

		@Override
		public int numBytes() {
			return 2 + 4;
		}

		@Override
		public void write(IoBuffer ioBuffer) {
			ioBuffer.putShort(shortValue);
			ioBuffer.putFloat(floatValue);
		}

		@Override
		public void read(IoBuffer ioBuffer) {
			this.shortValue = ioBuffer.getShort();
			this.floatValue = ioBuffer.getFloat();
		}

		@Override
		public int read(byte[] byteArray, int index) {
			shortValue = new Integer(
				((byteArray[index] & 0xff) << 8) | (byteArray[index + 1] & 0xff)).shortValue();
			floatValue = Float.intBitsToFloat((((byteArray[index + 2] & 0xff)) << 24) |
				(((byteArray[index + 3] & 0xff)) << 16) |
				(((byteArray[index + 4] & 0xff)) << 8) |
				((byteArray[index + 5] & 0xff)));
			return 6;
		}

		@Override
		public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
			ioBuffer.get(byteArray, index, 6);
			return 6;
		}

		@Override
		public String toString() {
			return "shortValue: " + shortValue + " floatValue: " + floatValue;
		}

		@Override
		public void combine(byte[] messageQueue, byte[] tmpArray) {
			// Nothing to do
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return SumOfNeighborsShortAndFloatValuesVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return SumOfNeighborsShortAndFloatValuesVertex.class;
		}

		@Override
		public Class<?> getVertexValueClass() {
			return SumOfNeighborsShortAndFloatValuesVertex.TestWritable.class;
		}

		@Override
		public Class<?> getMessageValueClass() {
			return SumOfNeighborsShortAndFloatValuesVertex.TestWritable.class;
		}
	}
}
