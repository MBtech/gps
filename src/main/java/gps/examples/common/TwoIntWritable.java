package gps.examples.common;

import gps.writable.MinaWritable;

import org.apache.mina.core.buffer.IoBuffer;

public class TwoIntWritable extends MinaWritable {

	public byte type;
	public int intValue1;
	public int intValue2;

	public TwoIntWritable() {
		// Every writable that will be used as a message needs to implement a default
		// constructor.
	}

	public TwoIntWritable(int intValue1) {
		this.intValue1 = intValue1;
		this.type = 0;
	}

	public TwoIntWritable(int intValue1, int intValue2) {
		this.intValue1 = intValue1;
		this.intValue2 = intValue2;
		this.type = 1;
	}

	@Override
	public int numBytes() {
		return type == 0 ? 5 : 9;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.put(type);
		ioBuffer.putInt(intValue1);
		if (type == 1) {
			ioBuffer.putInt(intValue2);
		}
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		// Nothing to do for now. Implment this when this will be used as a GO.
	}

	@Override
	public int read(byte[] byteArray, int index) {
		this.type = byteArray[index];
		this.intValue1 = readIntegerFromByteArray(byteArray, index + 1);
		if (type == 1) {
			this.intValue2 = readIntegerFromByteArray(byteArray, index + 5);
		}
		return this.type == 0 ? 5 : 9;
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		byte messageType = ioBuffer.get();
		byteArray[index] = messageType;
		ioBuffer.get(byteArray, index + 1, 4);
		if (messageType == 1) {
			ioBuffer.get(byteArray, index + 5, 4);
		}
		return messageType == 0 ? 5 : 9;
	}

	@Override
	public void combine(byte[] messageQueue, byte[] tmpArray) {
		// Nothing to do. This writable is not combinable.
	}
}
