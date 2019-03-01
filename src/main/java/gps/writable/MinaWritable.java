package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;

public abstract class MinaWritable {

	// TODO(semih): remove this, so that non-fixed size messages are allowed as well.
	// Called when serializing a writable object and also when creating the message
	// queues to get an approximate size of the initial sizes of the queues.
	public abstract int numBytes();

	// Write the data in this writable into an IoBuffer
	// Called when serializing the writable (either as a message or as a BV)
	public abstract void write(IoBuffer ioBuffer);
	
	// Read and convert the data (as much as needed) from the ioBuffer into this Writable
	// Fill this in only if this writable will be used as a BV.
	public abstract void read(IoBuffer ioBuffer);
	
	// Read and convert the data (as much as needed) from this byte array into this Writable,
	// starting from the given index of the byteArray.
	// Called when iterating over messages
	public abstract int read(byte[] byteArray, int index);
	
	// This should be implemented by algorithms that need to add edges hat have values on them to the graph
	// during the computation.
	public byte[] getByteArray() {
		return null;
	}

	public void read(String strValue) {
		// By default do nothing
	}

	// Used for parsing input file's vertex value
	public void readVertexValue(String strValue, int source) {
		read(strValue);
	}
	
	// Used for parsing input file's edge value
	public void readEdgeValue(String strValue, int source, int neighborId) {
		read(strValue);
	}

	// Copy data (as much as needed) from the IoBuffer to the byte array starting from the given
	// index of the byteArray.
	// Called on the receiver side when the message is in the ioBuffer and will be put into
	// the message queue byteArray.
	public abstract int read(IoBuffer ioBuffer, byte[] byteArray, int index);
	
	// If this writable is combinable, combine the message encoded in tmpArray
	// and the current representation of this writable in the messageQueue and then
	// write it back to messageQueue.
	public void combine(byte[] messageQueue, byte[] tmpArray) {
		// nothing to combine
	}
	
	public boolean hasFixedSize() {
		return true;
	}

	protected long readLongFromByteArray(byte[] byteArray, int index) {
		return (((long) (byteArray[index] & 0xff)) << 56) |
		(((long) (byteArray[index + 1] & 0xff)) << 48) |
		(((long) (byteArray[index + 2] & 0xff)) << 40) |
		(((long) (byteArray[index + 3] & 0xff)) << 32) |
		(((long) (byteArray[index + 4] & 0xff)) << 24) |
		(((long) (byteArray[index + 5] & 0xff)) << 16) |
		(((long) (byteArray[index + 6] & 0xff)) << 8) |
		((long) (byteArray[index + 7] & 0xff));
	}

	protected void writeLongToByteArrayFromIndexZero(byte[] messageQueue, long longValue, int index) {
		messageQueue[index] = (byte) (((byte) (longValue >> 56)) & 0xff);
		messageQueue[index + 1] = (byte) (((byte) (longValue >> 48)) & 0xff);
		messageQueue[index + 2] = (byte) (((byte) (longValue >> 40)) & 0xff);
		messageQueue[index + 3] = (byte) (((byte) (longValue >> 32)) & 0xff);
		messageQueue[index + 4] = (byte) (((byte) (longValue >> 24)) & 0xff);
		messageQueue[index + 5] = (byte) (((byte) (longValue >> 16)) & 0xff);
		messageQueue[index + 6] = (byte) (((byte) (longValue >> 8)) & 0xff);
		messageQueue[index + 7] = (byte) (((byte) (longValue >> 0)) & 0xff);
	}

	protected int readIntegerFromByteArray(byte[] byteArray, int index) {
		return ((byteArray[index] & 0xff) << 24) | ((byteArray[index + 1] & 0xff) << 16)
			| ((byteArray[index + 2] & 0xff) << 8) | (byteArray[index + 3] & 0xff);
	}

	protected void writeIntegerToByteArray(byte[] messageQueue, int intValue, int index) {
		messageQueue[index] = (byte) ((intValue >> 24) & 0xff);
		 messageQueue[index + 1] = (byte) ((intValue >> 16) & 0xff);
		 messageQueue[index + 2] = (byte) ((intValue >> 8) & 0xff);
		 messageQueue[index + 3] = (byte) (intValue & 0xff);
	}
}
