package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;

public class IntWritable extends MinaWritable {

	public int value;

	public IntWritable() {
	}

	public IntWritable(int value) {
		this.value = value;
	}

	@Override
	public int numBytes() {
		return 4;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putInt(value);
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		this.value = ioBuffer.getInt();
	}

	@Override
	public void read(String strValue) {
		this.value = Integer.parseInt(strValue);
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		ioBuffer.get(byteArray, index, 4);
		return 4;
	}

	public int getValue() {
		return value;
	}
	
	@Override
	public String toString() {
		return "-1 " + value;
	}

	@Override
	public int read(byte[] byteArray, int index) {
		this.value = readIntegerFromByteArray(byteArray, index);
		return 4;
	}

	@Override
	public void combine(byte[] messageQueue, byte[] tmpArray) {
		int combineValue = ((tmpArray[0] & 0xff) << 24) | ((tmpArray[1] & 0xff) << 16)
		| ((tmpArray[2] & 0xff) << 8) | (tmpArray[3] & 0xff);
		combineValue += ((messageQueue[0] & 0xff) << 24) | ((messageQueue[1] & 0xff) << 16)
		| ((messageQueue[2] & 0xff) << 8) | (messageQueue[3] & 0xff);
		writeIntegerToByteArray(messageQueue, combineValue, 0 /* start index */);
	}
}