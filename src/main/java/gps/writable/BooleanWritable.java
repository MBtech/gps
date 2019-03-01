package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;

public class BooleanWritable extends MinaWritable {

	private boolean value;

	public BooleanWritable() {
	}

	public BooleanWritable(boolean value) {
		this.value = value;
	}

	@Override
	public int numBytes() {
		return 1;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.put(value ? (byte) 1 : (byte) 0);
	}

	public void read(String strValue) {
		this.value = Boolean.parseBoolean(strValue);
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		this.value = getBooleanFromByte(ioBuffer.get());
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		ioBuffer.get(byteArray, index, 1);
		return 1;
	}

	public boolean getValue() {
		return value;
	}
	
	@Override
	public String toString() {
		return "" + value;
	}
	
	public void setValue(boolean value) {
		this.value = value;
	}

	@Override
	public int read(byte[] byteArray, int index) {
		this.value = getBooleanFromByte(byteArray[index]);
		return 1;
	}

	private boolean getBooleanFromByte(byte byteValue) {
		return byteValue == (byte) 0 ? false : true;
	}

	@Override
	public void combine(byte[] messageQueue, byte[] tmpArray) {
		throw new UnsupportedOperationException("Combining for booleans is not supported");
	}
}