package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;

public class IntBooleanWritable extends MinaWritable {

	public int intValue;
	public boolean booleanValue;

	public IntBooleanWritable() {
	}

	public IntBooleanWritable(int intValue, boolean booleanValue) {
		this.intValue = intValue;
		this.booleanValue = booleanValue;
	}

	@Override
	public int numBytes() {
		return 5;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putInt(intValue);
		ioBuffer.put(booleanValue ? (byte) 1 : (byte) 0);
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		this.intValue = ioBuffer.getInt();
		this.booleanValue = ioBuffer.get() == ((byte) 1) ? true : false;
	}


	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		ioBuffer.get(byteArray, index, 4);
		ioBuffer.get(byteArray, index + 4, 1);
		return 5;
	}

	public int getIntValue() {
		return intValue;
	}
	
	public boolean getBoolValue() {
		return booleanValue;
	}

	@Override
	public String toString() {
		return "intValue: " + intValue + " booleanValue: " + booleanValue;
	}

	@Override
	public int read(byte[] byteArray, int index) {
		this.intValue = readIntegerFromByteArray(byteArray, index);
		this.booleanValue = read(byteArray, index + 4) == (byte) 1 ? true : false;
		return 5;
	}
}