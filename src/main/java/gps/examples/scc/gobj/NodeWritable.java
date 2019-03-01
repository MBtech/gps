package gps.examples.scc.gobj;

import org.apache.mina.core.buffer.IoBuffer;

import gps.examples.scc.SCCVertexValue;
import gps.writable.MinaWritable;

public class NodeWritable extends MinaWritable {

	public SCCVertexValue value;
	public int[] neighbors;

	public NodeWritable() throws InstantiationException, IllegalAccessException {
		neighbors = new int[0];
		value = new SCCVertexValue();
	}

	public NodeWritable(SCCVertexValue value, int[] neighbors) {
		this.value = value;
		this.neighbors = neighbors;
	}

	@Override
	public int numBytes() {
		return 4 + (4*neighbors.length) + (value == null ? 8 : value.numBytes());
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putInt(neighbors.length);
		for (int neighbor : neighbors) {
			ioBuffer.putInt(neighbor);
		}
		value.write(ioBuffer);
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		int neighborsLength = ioBuffer.getInt();
		neighbors = new int[neighborsLength];
		for (int i = 0; i < neighborsLength; ++i) {
			neighbors[i] = ioBuffer.getInt();
		}
		value.read(ioBuffer);
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		throw new UnsupportedOperationException("reading from the io buffer into the byte[] should never" +
			" be called for this global object writable: " + getClass().getCanonicalName());
	}

	@Override
	public int read(byte[] byteArray, int index) {
		throw new UnsupportedOperationException("reading from the byte[] into java object should never " +
			" be called for this global object writable: " + getClass().getCanonicalName());
	}
}