package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;

public class FloatWritable extends MinaWritable {

	private float value;

	public FloatWritable() {
	}

	public FloatWritable(float value) {
		this.value = value;
	}

	@Override
	public int numBytes() {
		return 4;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putFloat(value);
	}
	
	@Override
	public void read(IoBuffer ioBuffer) {
		this.value = ioBuffer.getFloat();
	}

	public void read(String strValue) {
		this.value = Float.parseFloat(strValue);
	}

	@Override
	public String toString() {
		return "" + value;
	}
	
	public float getValue() {
		return value;
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		ioBuffer.get(byteArray, index, 4);
		return 4;
	}

	@Override
	public void combine(byte[] messageQueue, byte[] tmpArray) {
		float combineValue = Float.intBitsToFloat(
			((tmpArray[0] & 0xff) << 24) |
			((tmpArray[1] & 0xff) << 16) |
			((tmpArray[2] & 0xff) << 8) |
		    (tmpArray[3] & 0xff));
		combineValue += Float.intBitsToFloat(
			((messageQueue[0] & 0xff) << 24) |
			((messageQueue[1] & 0xff) << 16) |
			((messageQueue[2] & 0xff) << 8) |
			(messageQueue[3] & 0xff));
		int combineInt = Float.floatToIntBits(combineValue);
		writeIntegerToByteArray(messageQueue, combineInt, 0 /* start index */);
	}

	@Override
	public int read(byte[] byteArray, int index) {
		this.value = Float.intBitsToFloat(readIntegerFromByteArray(byteArray, index));
		return 4;
	}
}