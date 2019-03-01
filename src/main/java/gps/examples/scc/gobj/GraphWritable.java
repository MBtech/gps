package gps.examples.scc.gobj;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mina.core.buffer.IoBuffer;

import gps.writable.MinaWritable;

public class GraphWritable extends MinaWritable {
	
	public Map<Integer, NodeWritable> graph;

	public GraphWritable() {
		graph = new HashMap<Integer, NodeWritable>();
	}

	public GraphWritable(Map<Integer, NodeWritable> graph) {
		this.graph = graph;
	}

	@Override
	public int numBytes() {
		if (graph != null && !graph.isEmpty()) {
			int numBytes = 4; // for num-entries
			for (Entry<Integer, NodeWritable> entry : graph.entrySet()) {
				numBytes += 4 + entry.getValue().numBytes();
			}
			return numBytes;
		}
		return 1000;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putInt(graph.size());
		for (Entry<Integer, NodeWritable> entry : graph.entrySet()) {
			ioBuffer.putInt(entry.getKey());
			entry.getValue().write(ioBuffer);
		}
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		graph = new HashMap<Integer, NodeWritable>();
		int numVertices = ioBuffer.getInt();
		int vertexId;
		NodeWritable vertex;
		for (int i = 0; i < numVertices; ++i) {
			vertexId = ioBuffer.getInt();
			try {
				vertex = NodeWritable.class.newInstance();
				vertex.read(ioBuffer);
				graph.put(vertexId, vertex);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
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
