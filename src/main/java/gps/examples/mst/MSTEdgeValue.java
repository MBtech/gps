package gps.examples.mst;

import org.apache.mina.core.buffer.IoBuffer;

import gps.writable.MinaWritable;

public class MSTEdgeValue extends MinaWritable {

	protected int originalFromId;
	protected int originalToId;
	protected double weight;

	public MSTEdgeValue() {
	}

	@Override
	public int numBytes() {
		// 4 for the original id of the neighbor
		// 8 bytes for the weight)
		return 4 + 4 + 8;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putInt(this.originalFromId);
		ioBuffer.putInt(this.originalToId);
		ioBuffer.putDouble(this.weight);
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		this.originalFromId = ioBuffer.getInt();
		this.originalToId = ioBuffer.getInt();
		this.weight = ioBuffer.getDouble();
	}

	@Override
	public void readEdgeValue(String strValue, int source, int neighborId) {
		this.originalFromId = source;
		this.originalToId = neighborId;
		this.weight = Double.parseDouble(strValue);
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		ioBuffer.get(byteArray, index, 4);
		ioBuffer.get(byteArray, index + 4, 4);
		ioBuffer.get(byteArray, index + 8, 8);
		return 4 + 4 + 8;
	}

	@Override
	public int read(byte[] byteArray, int index) {
		this.originalFromId = readIntegerFromByteArray(byteArray, index);
		this.originalToId = readIntegerFromByteArray(byteArray, index + 4);
		this.weight = Double.longBitsToDouble(readLongFromByteArray(byteArray, index + 8));
		return 4 + 4 + 8;
	}

	@Override
	public byte[] getByteArray() {
		byte[] retVal = new byte[numBytes()];
		writeIntegerToByteArray(retVal, originalFromId, 0);
		writeIntegerToByteArray(retVal, originalToId, 4);
		writeLongToByteArrayFromIndexZero(retVal, Double.doubleToLongBits(weight), 4 + 4);
		return retVal;
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("originalFromId: " + originalFromId + "\n");
		stringBuilder.append("originalToId: " + originalToId + "\n");
		stringBuilder.append("weight: " + weight + "\n");
		return stringBuilder.toString();
	}
}