package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;

/**
 * Place-holder class to be used as a type in parameterized classes.
 * 
 * @author semihsalihoglu
 */
public class NullWritable extends MinaWritable {

	@Override
	public int numBytes() {
		return 0;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		// Nothing to do.
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		// Nothing to do.
	}

	@Override
	public int read(byte[] byteArray, int index) {
		return 0;
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		return 0;
	}

	@Override
	public void combine(byte[] messageQueue, byte[] tmpArray) {
		// Nothing to do.
	}
}