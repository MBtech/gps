package gps.examples.mst;

import java.util.HashMap;
import java.util.Map;

import org.apache.mina.core.buffer.IoBuffer;

import gps.writable.MinaWritable;

public class MSTMessageValue extends MinaWritable {

	protected MSTMessageType type;
	protected int int1;
	protected int int2;
	protected int int3;
	protected double double1;

	public MSTMessageValue() {
		type = MSTMessageType.POINTING_AT_YOU_MESSAGE;
	}

	public MSTMessageValue(MSTMessageType type, int int1) {
		this.type = type;
		this.int1 = int1;
	}
	
	public MSTMessageValue(MSTMessageType type, int int1, int int2) {
		this.type = type;
		this.int1 = int1;
		this.int2 = int2;
	}

	public MSTMessageValue(int int1, int int2, int int3, double double1) {
		this.type = MSTMessageType.NEW_EDGE_MESSAGE;
		this.int1 = int1;
		this.int2 = int2;
		this.int3 = int3;
		this.double1 = double1;
	}

	@Override
	public int numBytes() {
		if (type == MSTMessageType.NEW_EDGE_MESSAGE) {
			return 1 + 4 + 4 + 4 + 8;
		} if (type == MSTMessageType.NEW_SUPERNODE_MESSAGE) {
			return 1 + 4 + 4;
		} else {
			return 1 + 4;
		}
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.put(type.id);
		ioBuffer.putInt(int1);
		if (type == MSTMessageType.NEW_SUPERNODE_MESSAGE) {
			ioBuffer.putInt(int2);
		} else if (type == MSTMessageType.NEW_EDGE_MESSAGE) {
			ioBuffer.putInt(int2);
			ioBuffer.putInt(int3);
			ioBuffer.putDouble(double1);
		}
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		type = MSTMessageType.getMSTMessageTypeFromId(ioBuffer.get());
		int1 = ioBuffer.getInt();
		if (type == MSTMessageType.NEW_SUPERNODE_MESSAGE) {
			int2 = ioBuffer.getInt();
		} else if (type == MSTMessageType.NEW_EDGE_MESSAGE) {
			int2 = ioBuffer.getInt();
			int3 = ioBuffer.getInt();
			double1 = ioBuffer.getDouble();
		}
	}

	@Override
	public void read(String string1) {
		throw new UnsupportedOperationException("This method should not be called because MSTMessages " +
			"should not be parsed from a file.");
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
        byteArray[index] = ioBuffer.get();
        this.type = MSTMessageType.getMSTMessageTypeFromId(byteArray[index]);
		ioBuffer.get(byteArray, index + 1, 4);
		if (this.type == MSTMessageType.NEW_SUPERNODE_MESSAGE) {
			ioBuffer.get(byteArray, index + 5, 4);
			return 1 + 4 + 4;
		} else if (this.type == MSTMessageType.NEW_EDGE_MESSAGE) {
			ioBuffer.get(byteArray, index + 5, 4);
			ioBuffer.get(byteArray, index + 9, 4);
			ioBuffer.get(byteArray, index + 13, 8);
			return 1 + 4 + 4 + 4 + 8;
		}
		return 1 + 4;
	}

	@Override
	public int read(byte[] byteArray, int index) {
        this.type = MSTMessageType.getMSTMessageTypeFromId(byteArray[index]);
		this.int1 = readIntegerFromByteArray(byteArray, index + 1);
		if (type == MSTMessageType.NEW_SUPERNODE_MESSAGE) {
			this.int2 = readIntegerFromByteArray(byteArray, index + 5);
			return 1 + 4 + 4;
		} else if (type == MSTMessageType.NEW_EDGE_MESSAGE) {
			this.int2 = readIntegerFromByteArray(byteArray, index + 5);
			this.int3 = readIntegerFromByteArray(byteArray, index + 9);
			this.double1 = Double.longBitsToDouble(readLongFromByteArray(byteArray, index + 13));
			return 1 + 4 + 4 + 4 + 8;
		}
		return 1 + 4;
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("type: " + type + "\n");
			stringBuilder.append("int1: " + int1 + "\n");
			if (type == MSTMessageType.NEW_SUPERNODE_MESSAGE) {
				stringBuilder.append("int2: " + int2 + "\n");
			} else if (type == MSTMessageType.NEW_EDGE_MESSAGE) {
				stringBuilder.append("int2: " + int2 + "\n");
				stringBuilder.append("int3: " + int3 + "\n");
				stringBuilder.append("double1: " + double1 + "\n");
			}

		return stringBuilder.toString();
	}

	public static enum MSTMessageType {
		POINTING_AT_YOU_MESSAGE((byte) 0),
		IS_ROOT_MESSAGE((byte) 1),
		NEW_PARENT_MESSAGE((byte) 2),
		NEW_SUPERNODE_MESSAGE((byte) 3),
		NEW_EDGE_MESSAGE((byte) 4),
		NEW_SUBVERTEX_MESSAGE((byte) 5);

		private static Map<Byte, MSTMessageType> idMSTMessageTypeMap =
			new HashMap<Byte, MSTMessageType>();
		static {
			for (MSTMessageType type : MSTMessageType.values()) {
				idMSTMessageTypeMap.put(type.id, type);
			}
		}

		private byte id;

		private MSTMessageType(byte id) {
			this.id = id;
		}

		public byte getId() {
			return id;
		}

		public static MSTMessageType getMSTMessageTypeFromId(byte id) {
			return idMSTMessageTypeMap.get(id);
		}
	}

	public static MSTMessageValue newPointingAtYouMessage(int pickedNeighborId) {
		return new MSTMessageValue(MSTMessageType.POINTING_AT_YOU_MESSAGE, pickedNeighborId);
	}

	public static MSTMessageValue newParentMessage(int parentId) {
		return new MSTMessageValue(MSTMessageType.NEW_PARENT_MESSAGE, parentId);
	}

	public static MSTMessageValue newIsRootMessage(int rootId) {
		return new MSTMessageValue(MSTMessageType.IS_ROOT_MESSAGE, rootId);
	}

	public static MSTMessageValue newSubvertexMessage(int subVertexId) {
		return new MSTMessageValue(MSTMessageType.NEW_SUBVERTEX_MESSAGE, subVertexId);
	}

	public static MSTMessageValue newECODQuestionMessage(int int1, int int2) {
		return newSupernodeMessage(int1, int2);
	}

	public static MSTMessageValue newECODAnswerMessage(int pickedEdgeId) {
		return newIsRootMessage(pickedEdgeId);
	}

	public static MSTMessageValue newSupernodeMessage(int int1, int int2) {
		return new MSTMessageValue(MSTMessageType.NEW_SUPERNODE_MESSAGE, int1, int2);
	}
 
	public static MSTMessageValue newEdgeMessage(int newToId, int originalFromId, int originalToId,
		double weight) {
		return new MSTMessageValue(newToId, originalFromId, originalToId, weight);
	}

	public static MSTMessageValue newECODEdgeCleaning1Message(int id) {
		return newIsRootMessage(id);
	}

	public static MSTMessageValue newECODEdgeCleaning2Message(int id, int parent) {
		return newSupernodeMessage(id, parent);
	}
}