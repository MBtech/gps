package gps.examples.scc;

import java.util.HashMap;
import java.util.Map;

import org.apache.mina.core.buffer.IoBuffer;

import gps.writable.MinaWritable;

/**
 * Encapsulates the messages sent in different phases of different SCC algorithms.
 *
 * @author semihsalihoglu
 */
public class SCCMessageValue extends MinaWritable {

	public SCCMessageType type;
	public int int1;
	public int int2;
	public byte byte1;

	public SCCMessageValue() {
		type = SCCMessageType.REVERSE_GRAPH_FORMATION;
	}

	public SCCMessageValue(int int1, SCCMessageType type) {
		this.type = type;
		this.int1 = int1;
	}

	public SCCMessageValue(byte rootIndex, SCCMessageType type) {
		this.type = type;
		this.byte1 = rootIndex;
	}

	public SCCMessageValue(int fromId, int fwId) {
		this.type = SCCMessageType.EDGE_INT_CLEANING_MESSAGE;
		this.int1 = fromId;
		this.int2 = fwId;
	}

	public SCCMessageValue(int fromId, byte fwId) {
		this.type = SCCMessageType.EDGE_BYTE_CLEANING_MESSAGE;
		this.int1 = fromId;
		this.byte1 = fwId;
	}

	@Override
	public int numBytes() {
		if (type == SCCMessageType.REVERSE_GRAPH_FORMATION || type == SCCMessageType.INT_TRAVERSAL_ID_MESSAGE) {
			return 1 + 4;
		} else if (type == SCCMessageType.BYTE_TRAVERSAL_MESSAGE) {
			return 1 + 1;
		} else if (type == SCCMessageType.EDGE_INT_CLEANING_MESSAGE) {
			return 1 + 4 + 4;
		} else if (type == SCCMessageType.EDGE_BYTE_CLEANING_MESSAGE) {
			return 1 + 4 + 1;
		} else {
			return -1;
		}
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.put(type.id);
		if (type == SCCMessageType.REVERSE_GRAPH_FORMATION || type == SCCMessageType.INT_TRAVERSAL_ID_MESSAGE) {
			ioBuffer.putInt(int1);
		} else if (type == SCCMessageType.BYTE_TRAVERSAL_MESSAGE) {
			ioBuffer.put(byte1);
		} else if (type == SCCMessageType.EDGE_INT_CLEANING_MESSAGE) {
			ioBuffer.putInt(int1);
			ioBuffer.putInt(int2);
		} else if (type == SCCMessageType.EDGE_BYTE_CLEANING_MESSAGE) {
			ioBuffer.putInt(int1);
			ioBuffer.put(byte1);
		}
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		type = SCCMessageType.getMSTMessageTypeFromId(ioBuffer.get());
		if (type == SCCMessageType.REVERSE_GRAPH_FORMATION || type == SCCMessageType.INT_TRAVERSAL_ID_MESSAGE) {
			int1 = ioBuffer.getInt();
		} else if (type == SCCMessageType.BYTE_TRAVERSAL_MESSAGE) {
			byte1 = ioBuffer.get();
		} else if (type == SCCMessageType.EDGE_INT_CLEANING_MESSAGE) {
			int1 = ioBuffer.getInt();
			int2 = ioBuffer.getInt();
		} else if (type == SCCMessageType.EDGE_BYTE_CLEANING_MESSAGE) {
			int1 = ioBuffer.getInt();
			byte1 = ioBuffer.get();
		}
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
        byteArray[index] = ioBuffer.get();
        type = SCCMessageType.getMSTMessageTypeFromId(byteArray[index]);
        if (type == SCCMessageType.REVERSE_GRAPH_FORMATION || type == SCCMessageType.INT_TRAVERSAL_ID_MESSAGE) {
        	ioBuffer.get(byteArray, index + 1, 4);
        	return 1 + 4;
		} else if (type == SCCMessageType.BYTE_TRAVERSAL_MESSAGE) {
        	ioBuffer.get(byteArray, index + 1, 1);
        	return 1 + 1;
		} else if (type == SCCMessageType.EDGE_INT_CLEANING_MESSAGE) {
        	ioBuffer.get(byteArray, index + 1, 4);
        	ioBuffer.get(byteArray, index + 5, 4);
        	return 1 + 4 + 4;
		}  else if (type == SCCMessageType.EDGE_BYTE_CLEANING_MESSAGE) {
        	ioBuffer.get(byteArray, index + 1, 4);
        	ioBuffer.get(byteArray, index + 5, 1);
        	return 1 + 4 + 1;
		} else {
			return -1;
		}
	}

	@Override
	public int read(byte[] byteArray, int index) {
        type = SCCMessageType.getMSTMessageTypeFromId(byteArray[index]);
        if (type == SCCMessageType.REVERSE_GRAPH_FORMATION || type == SCCMessageType.INT_TRAVERSAL_ID_MESSAGE) {
			int1 = readIntegerFromByteArray(byteArray, index + 1);
			return 1 + 4;
        } else if (type == SCCMessageType.BYTE_TRAVERSAL_MESSAGE) {
			byte1 = byteArray[index + 1];
			return 1 + 1;
        } else if (type == SCCMessageType.EDGE_INT_CLEANING_MESSAGE) {
        	int1 = readIntegerFromByteArray(byteArray, index + 1);
        	int2 = readIntegerFromByteArray(byteArray, index + 5);
        	return 1 + 4 + 4;
        } else if (type == SCCMessageType.EDGE_BYTE_CLEANING_MESSAGE) {
        	int1 = readIntegerFromByteArray(byteArray, index + 1);
        	byte1 = byteArray[index + 5];
        	return 1 + 4 + 1;
        }
		return -1;
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("type: " + type + "\n");
		if (type == SCCMessageType.REVERSE_GRAPH_FORMATION || type == SCCMessageType.INT_TRAVERSAL_ID_MESSAGE) {
			stringBuilder.append("int1: " + int1 + "\n");
		} else if (type == SCCMessageType.BYTE_TRAVERSAL_MESSAGE) {
			stringBuilder.append("byte1: " + byte1 + "\n");
		} else if (type == SCCMessageType.EDGE_INT_CLEANING_MESSAGE) {
			stringBuilder.append("int1: " + int1 + "\n");
			stringBuilder.append("int2: " + int2 + "\n");
		} else if (type == SCCMessageType.EDGE_BYTE_CLEANING_MESSAGE) {
			stringBuilder.append("int1: " + int1 + "\n");
			stringBuilder.append("byte1: " + byte1 + "\n");
		}
		return stringBuilder.toString();
	}

	public static enum SCCMessageType {
		REVERSE_GRAPH_FORMATION((byte) 0),
		BYTE_TRAVERSAL_MESSAGE((byte) 1),
		EDGE_INT_CLEANING_MESSAGE((byte) 2),
		EDGE_BYTE_CLEANING_MESSAGE((byte) 3),
		INT_TRAVERSAL_ID_MESSAGE((byte) 4);

		private static Map<Byte, SCCMessageType> idMSTMessageTypeMap =
			new HashMap<Byte, SCCMessageType>();
		static {
			for (SCCMessageType type : SCCMessageType.values()) {
				idMSTMessageTypeMap.put(type.id, type);
			}
		}

		private byte id;

		private SCCMessageType(byte id) {
			this.id = id;
		}

		public byte getId() {
			return id;
		}

		public static SCCMessageType getMSTMessageTypeFromId(byte id) {
			return idMSTMessageTypeMap.get(id);
		}
	}

	public static SCCMessageValue newIntTraversalIdMessage(int id) {
		return new SCCMessageValue(id, SCCMessageType.INT_TRAVERSAL_ID_MESSAGE);
	}

	public static SCCMessageValue newReverseGraphFormationMessage(int id) {
		return new SCCMessageValue(id, SCCMessageType.REVERSE_GRAPH_FORMATION);
	}

	public static SCCMessageValue newByteTraversalMessage(byte rootIndex) {
		return new SCCMessageValue(rootIndex, SCCMessageType.BYTE_TRAVERSAL_MESSAGE);
	}

	public static SCCMessageValue newIntEdgeCleaningMessage(int fromId, int fwId) {
		return new SCCMessageValue(fromId, fwId);
	}
	
	public static SCCMessageValue newByteEdgeCleaningMessage(int fromId, byte fwId) {
		return new SCCMessageValue(fromId, fwId);
	}
}