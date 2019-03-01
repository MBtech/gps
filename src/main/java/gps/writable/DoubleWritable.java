package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;

public class DoubleWritable extends MinaWritable {
	
	private double value;

	public DoubleWritable() {
	}

	public DoubleWritable(double value) {
		this.value = value;
	}

	@Override
	public int numBytes() {
		return 8;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putDouble(value);
	}
	
	@Override
	public void read(IoBuffer ioBuffer) {
		this.value = ioBuffer.getDouble();
	}
	
	@Override
	public String toString() {
		return "" + value;
	}
	
	public double getValue() {
		return value;
	}

	@Override
	public void read(String strValue) {
		this.value = Double.parseDouble(strValue);
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		ioBuffer.get(byteArray, index, 8);
		return 8;
	}

	@Override
	public void combine(byte[] messageQueue, byte[] tmpArray) {
		double combineValue = Double.longBitsToDouble(readLongFromByteArray(tmpArray, 0));
		combineValue += Double.longBitsToDouble(readLongFromByteArray(messageQueue, 0));
		writeLongToByteArrayFromIndexZero(messageQueue, Double.doubleToLongBits(combineValue), 0);
	}

	@Override
	public int read(byte[] byteArray, int index) {
		this.value = Double.longBitsToDouble(readLongFromByteArray(byteArray, index));
		return 8;
	}
}