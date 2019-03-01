package gps.examples.mis;

import java.util.HashMap;
import java.util.Map;

import org.apache.mina.core.buffer.IoBuffer;

import gps.writable.MinaWritable;

public class MISVertexValue extends MinaWritable {

	protected MISVertexType type;
	protected int numRemainingNeighbors;
	
	public MISVertexValue() {
		this.type = MISVertexType.UNDECIDED;
	}

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
		return "" + type;
	}

	public static enum MISVertexType {
		IN_SET((byte) 0),
		NOT_IN_SET((byte) 1),
		SELECTED_AS_POSSIBLE_IN_SET((byte) 2),
		UNDECIDED((byte) 3);

		private static Map<Byte, MISVertexType> idMISVertexTypeMap = new HashMap<Byte, MISVertexType>();
		static {
			for (MISVertexType type : MISVertexType.values()) {
				idMISVertexTypeMap.put(type.id, type);
			}
		}

		private byte id;

		private MISVertexType(byte id) {
			this.id = id;
		}

		public byte getId() {
			return id;
		}

		public static MISVertexType getMISVertexTypeFromId(byte id) {
			return idMISVertexTypeMap.get(id);
		}
	}
}