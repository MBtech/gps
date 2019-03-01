package gps.writable;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


import org.apache.mina.core.buffer.IoBuffer;

public class StreamingRandomRootPickerMapWritable extends MinaWritable {

	private int k;
	public Map<Integer, StreamingRootPickerValue> keyValueMap;

	public StreamingRandomRootPickerMapWritable() {
		this.k = -1;
		keyValueMap = new HashMap<Integer, StreamingRootPickerValue>();
	}

	public StreamingRandomRootPickerMapWritable(int k) {
		this.k = k;
		keyValueMap = new HashMap<Integer, StreamingRootPickerValue>();
	}

	public StreamingRandomRootPickerMapWritable(
		Map<Integer, StreamingRootPickerValue> componentRootsMap, int k) {
		this.keyValueMap = componentRootsMap;
		this.k = k;
	}

	@Override
	public int numBytes() {
		// 4 for k, 4 for size
		int numBytes = 4 + 4;
		for (StreamingRootPickerValue value : keyValueMap.values()) {
			// 4 for componentId
			// 4 for numValuesInserted
			numBytes += 4 + 4;
			if (value.numValuesInserted < value.kValues.length) {
				numBytes += 4 * value.numValuesInserted;
			} else {
				numBytes += 4 * value.kValues.length;
			}
		}
		return numBytes;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		System.out.println("Writing StreamingRandomRootPickerMapWritable to the IoBuffer.");
		ioBuffer.putInt(k);
		ioBuffer.putInt(keyValueMap.size());
		Integer key;
		StreamingRootPickerValue value;
		for (Entry<Integer, StreamingRootPickerValue> keyValue : keyValueMap.entrySet()) {
			key = keyValue.getKey();
			value = keyValue.getValue();
			ioBuffer.putInt(key);
			ioBuffer.putInt(value.numValuesInserted);
			int numValuesToWrite = value.numValuesInserted < value.kValues.length ? value.numValuesInserted :
				value.kValues.length;
			for (int i = 0; i < numValuesToWrite; ++i) {
				ioBuffer.putInt(value.kValues[i]);
			}
		}
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		k = ioBuffer.getInt();
		keyValueMap = new HashMap<Integer, StreamingRootPickerValue>();
		int size = ioBuffer.getInt();
		System.out.println("Reading StreamingRandomRootPickerMapWritable from the IoBuffer. k: " + k + " size: " + size);
		Integer key = null;
		int numValuesInserted = -1;
		int[] kValues = null;
		int numValuesToReadFromIOBuffer;
		for (int i = 0; i < size; ++i) {
			key = ioBuffer.getInt();
			numValuesInserted = ioBuffer.getInt();
			kValues = new int[k];
			numValuesToReadFromIOBuffer = (numValuesInserted < k) ? numValuesInserted : k;
			for (int j = 0; j < numValuesToReadFromIOBuffer; ++j) {
				kValues[j] = ioBuffer.getInt();
			}
			keyValueMap.put(key, new StreamingRootPickerValue(kValues, numValuesInserted));
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

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		for (Entry<Integer, StreamingRootPickerValue> keyValue : keyValueMap.entrySet()) {
			stringBuilder.append(keyValue.getKey().toString() + " " + keyValue.getValue().toString());
		}
		return stringBuilder.toString();
	}

	@Override
	public void combine(byte[] messageQueue, byte[] tmpArray) {
		throw new UnsupportedOperationException("Combining for booleans is not supported");
	}
}