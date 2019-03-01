package gps.examples.scc;

import java.util.HashMap;
import java.util.Map;

import org.apache.mina.core.buffer.IoBuffer;

import gps.writable.MinaWritable;

/**
 * Encapsulates the value stored by vertices for different SCC algorithms.
 *
 * @author semihsalihoglu
 */
public class SCCVertexValue extends MinaWritable {

	public SCCVertexType type = null;
	public int colorID = -1;
	public byte bwId = -1;
	public int[] transposeNeighbors = null;

	public SCCVertexValue() {
		type = null;
		colorID = -1;
		bwId = -1;
		transposeNeighbors = null;
	}

	private SCCVertexValue(SCCVertexType type, int fwId, byte bwId, int[] reverseNeighbors) {
		this.type = type;
		this.colorID = fwId;
		this.bwId = bwId;
		if (reverseNeighbors != null) {
			this.transposeNeighbors = new int[reverseNeighbors.length];
			for (int i = 0; i < reverseNeighbors.length; ++i) {
				this.transposeNeighbors[i] = reverseNeighbors[i];
			}
		}
	}

	@Override
	public int numBytes() {
		int numBytes = 1 + 4 + 1 + 4; // the last 4 is for the length of reverseNeighbors
		if (transposeNeighbors != null) {
			numBytes += transposeNeighbors.length*4;
		}
		return numBytes;
	}
	
	public SCCVertexValue copy() {
		return new SCCVertexValue(type, colorID, bwId, transposeNeighbors);
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.put(type.id);
		ioBuffer.putInt(colorID);
		ioBuffer.put(bwId);
		if (transposeNeighbors != null) {
			ioBuffer.putInt(transposeNeighbors.length);
			for (int reverseNeighbor : transposeNeighbors) {
				ioBuffer.putInt(reverseNeighbor);
			}
		} else {
			ioBuffer.putInt(0);
		}
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		type = SCCVertexType.getSCCVertexTypeFromId(ioBuffer.get());
		colorID = ioBuffer.getInt();
		bwId = ioBuffer.get();
		transposeNeighbors = new int[ioBuffer.getInt()];
		for (int i = 0; i < transposeNeighbors.length; ++i) {
			transposeNeighbors[i] = ioBuffer.getInt();
		}
	}

	@Override
	public int read(byte[] byteArray, int index) {
		return 0;
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		return 0;
	}

	@Override
	public void combine(byte[] messageQueue, byte[] tmpArray) {}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("type: " + type.name());
		stringBuilder.append(" fwId: " + colorID);
		stringBuilder.append(" bwId: " + bwId);
		if (transposeNeighbors != null) {
			stringBuilder.append(" reverseNeighbors:");
			for (int reverseNeighbor : transposeNeighbors) {
				stringBuilder.append(", " + reverseNeighbor);
			}
		}
		return stringBuilder.toString();
	}

	public static enum SCCVertexType {
		ROOT((byte) 0),
		NON_ROOT((byte) 1),
		FOUND_COMPONENT((byte) 2),
		SINGLETON((byte) 3);

		private static Map<Byte, SCCVertexType> idMSTVertexTypeMap = new HashMap<Byte, SCCVertexType>();
		static {
			for (SCCVertexType type : SCCVertexType.values()) {
				idMSTVertexTypeMap.put(type.id, type);
			}
		}

		private byte id;

		private SCCVertexType(byte id) {
			this.id = id;
		}

		public byte getId() {
			return id;
		}

		public static SCCVertexType getSCCVertexTypeFromId(byte id) {
			return idMSTVertexTypeMap.get(id);
		}
	}
}
