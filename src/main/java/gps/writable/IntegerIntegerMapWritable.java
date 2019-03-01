package gps.writable;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mina.core.buffer.IoBuffer;

import gps.writable.MinaWritable;

public class IntegerIntegerMapWritable extends MinaWritable {

	public Map<Integer, Integer> integerIntegerMap;

	public IntegerIntegerMapWritable(Map<Integer, Integer> componentKeyIntegerMap) {
		this.integerIntegerMap = componentKeyIntegerMap;
	}

	@Override
	public int numBytes() {	
		return 4 + ((4 + 4) * integerIntegerMap.size());
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putInt(integerIntegerMap.size());
		for (Entry<Integer, Integer> keyValue : integerIntegerMap.entrySet()) {
			ioBuffer.putInt(keyValue.getKey());
			ioBuffer.putInt(keyValue.getValue());
		}
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		integerIntegerMap = new HashMap<Integer, Integer>();
		int size = ioBuffer.getInt();
		for (int i = 0; i < size; ++i) {
			integerIntegerMap.put(ioBuffer.getInt(), ioBuffer.getInt());
		}
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		throw new UnsupportedOperationException("reading from the io buffer into the byte[] should never" +
			" be called for this global object writable: " + getClass().getCanonicalName());
	}

	@Override
	public int read(byte[] byteArray, int index) {
		throw new UnsupportedOperationException("reading from the byte[] into java object should never " +
			" be called for this global object writable: " + getClass().getCanonicalName());
	}
}