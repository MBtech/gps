package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;


public class StreamingRootPickerWritable extends MinaWritable {

	public StreamingRootPickerValue rootPicker;

	public StreamingRootPickerWritable() {
		this.rootPicker = new StreamingRootPickerValue(1);
	}

	public StreamingRootPickerWritable(StreamingRootPickerValue rootPicker) {
		this.rootPicker = rootPicker;
	}
	
	public StreamingRootPickerWritable(int k) {
		this.rootPicker = new StreamingRootPickerValue(k);
	}

	@Override
	public int numBytes() {
		// 4 for k, 4 for numValuesInserted
		int numBytes = 4 + 4;
		if (rootPicker.numValuesInserted < rootPicker.kValues.length) {
			numBytes += 4 * rootPicker.numValuesInserted;
		} else {
			numBytes += 4 * rootPicker.kValues.length;
		}
		return numBytes;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putInt(rootPicker.kValues.length);
		ioBuffer.putInt(rootPicker.numValuesInserted);
		int numValuesToWrite = rootPicker.numValuesInserted < rootPicker.kValues.length
			? rootPicker.numValuesInserted : rootPicker.kValues.length;
		for (int i = 0; i < numValuesToWrite; ++i) {
			ioBuffer.putInt(rootPicker.kValues[i]);
		}
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		int k = ioBuffer.getInt();
		int numValuesInserted = ioBuffer.getInt();
		int[] kValues = new int[k];
		int numValuesToReadFromIOBuffer = (numValuesInserted < k) ? numValuesInserted : k;		
		for (int j = 0; j < numValuesToReadFromIOBuffer; ++j) {
			kValues[j] = ioBuffer.getInt();
		}
		rootPicker = new StreamingRootPickerValue(kValues, numValuesInserted);
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
		return rootPicker.toString();
	}

	@Override
	public void combine(byte[] messageQueue, byte[] tmpArray) {
		throw new UnsupportedOperationException("Combining for booleans is not supported");
	}
}
