package gps.node;

import gps.writable.MinaWritable;

/**
 * Iterator to iterate over objects extending {@link MinaWritable}.
 * 
 * @author semihsalihoglu
 *
 * @param <W> Writable type that will be iterated over
 */
public class MinaWritableIterator<W extends MinaWritable> {

	protected byte[] writableBytes = null;
	public int numWritableBytes = -1;
	protected int currentByteLocation = -1;
	// TODO(semih): Make sure this is set
	public W representativeWritableInstance = null;

	public void setRepresentativeWritableInstance(W representativeWritableInstance) {
		this.representativeWritableInstance = representativeWritableInstance;
	}
	
	public boolean hasNext() {
		return currentByteLocation < numWritableBytes;
	}

	public void remove() {
		throw new UnsupportedOperationException(this.getClass().getName() +
			" does not support removing elements.");
	}

	public void init(byte[] writableBytes, int numWritableBytes) {
		this.writableBytes = writableBytes;
		this.numWritableBytes = numWritableBytes;
		this.currentByteLocation = 0;
	}

	public int getNumWritableBytes() {
		return numWritableBytes;
	}

	public void reset() {
		this.currentByteLocation = 0;
	}
}
