package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;

public class LongWritable extends MinaWritable {

	public long value;

	public LongWritable() {
	}

	public LongWritable(long value) {
		this.value = value;
	}

	@Override
	public int numBytes() {
		return 8;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putLong(value);
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		this.value = ioBuffer.getLong();
	}

	public void read(String strValue) {
		this.value = Long.parseLong(strValue);
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		ioBuffer.get(byteArray, index, 8);
		return 8;
	}

	public long getValue() {
		return value;
	}
	
	@Override
	public String toString() {
		return "" + value;
	}

	@Override
	public int read(byte[] byteArray, int index) {
		this.value = readLongFromByteArray(byteArray, index);
		return 8;
	}

	@Override
	public void combine(byte[] messageQueue, byte[] tmpArray) {
		long combineValue = readLongFromByteArray(tmpArray, 0);
		combineValue += readLongFromByteArray(messageQueue, 0);
		writeLongToByteArrayFromIndexZero(messageQueue, combineValue, 0);
	}
}