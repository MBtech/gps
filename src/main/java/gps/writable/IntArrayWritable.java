package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;

public class IntArrayWritable extends MinaWritable {

	public int[] value;

	public IntArrayWritable() {
		this.value = new int[0];
	}

	public IntArrayWritable(int[] value) {
		this.value = value;
	}

	@Override
	public int numBytes() {
		return 4 + 4*value.length;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putInt(value.length);
		for (int integerValue : value) {
			ioBuffer.putInt(integerValue);
		}
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		int length = ioBuffer.getInt();
		this.value = new int[length];
		for (int i = 0; i < length; ++i) {
			this.value[i] = ioBuffer.getInt();
		}
	}

	@Override
	public int read(byte[] byteArray, int index) {
		int length = readIntegerFromByteArray(byteArray, index);
		this.value = new int[length];
		for (int i = 0; i < length; ++i) {
			index += 4;
			this.value[i] = readIntegerFromByteArray(byteArray, index);
		}
		return 4 + (4*length);
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		int length = ioBuffer.getInt();
		writeIntegerToByteArray(byteArray, length, index);
		for (int i = 0; i < length; ++i) {			
			index += 4;
			ioBuffer.get(byteArray, index, 4);
		}
		return 4 + (4*length);
	}

	@Override
	public void combine(byte[] messageQueue, byte[] tmpArray) {
		// Nothing to do. This writable is not combinable.
	}
}
