package gps.examples.mst;

import java.util.HashMap;
import java.util.Map;

import org.apache.mina.core.buffer.IoBuffer;

import gps.writable.MinaWritable;

public class MSTVertexValue extends MinaWritable {

	protected MSTVertexType type = null;
	protected int parent = -1;
	protected int pickedEdgeOriginalFromId = -1;
	protected int pickedEdgeOriginalToId = -1;
	protected double pickedEdgeWeight = -1;

	@Override
	public int numBytes() {
		return 0;
	}

	@Override
	public void write(IoBuffer ioBuffer) {}

	@Override
	public void read(IoBuffer ioBuffer) {}

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
		stringBuilder.append(" pickedEdgeOriginalFromId: " + pickedEdgeOriginalFromId);
		stringBuilder.append(" pickedEdgeOriginalToId: " + pickedEdgeOriginalToId);
		stringBuilder.append(" pickedEdgeWeight: " + pickedEdgeWeight);
		return stringBuilder.toString();
	}

	public static enum MSTVertexType {
		ROOT((byte) 0),
		POINTS_AT_ROOT((byte) 1),
		POINTS_AT_NONROOT_VERTEX((byte) 2),
		SUB_VERTEX((byte) 3),
		ERROR_VERTEX((byte) 4),
		SUB_VERTEX_NOT_PICKED_MIN_EDGE((byte) 5),
		SUB_VERTEX_PICKED_MIN_EDGE((byte) 6),
		ROOT_NOT_PICKED_MIN_EDGE((byte) 7),
		ROOT_PICKED_MIN_EDGE((byte) 8),
		CERTAINLY_INACTIVE_VERTEX((byte) 9);

		private static Map<Byte, MSTVertexType> idMSTVertexTypeMap = new HashMap<Byte, MSTVertexType>();
		static {
			for (MSTVertexType type : MSTVertexType.values()) {
				idMSTVertexTypeMap.put(type.id, type);
			}
		}

		private byte id;

		private MSTVertexType(byte id) {
			this.id = id;
		}

		public byte getId() {
			return id;
		}

		public static MSTVertexType getMSTVertexTypeFromId(byte id) {
			return idMSTVertexTypeMap.get(id);
		}
	}
}
