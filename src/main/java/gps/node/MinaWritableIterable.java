package gps.node;

import gps.writable.MinaWritable;

import java.util.Iterator;

/**
 * {@link java.lang.Iterable} implementation of iterating over a list of {@link MinaWritable}
 * objects.
 * 
 * @author semihsalihoglu
 *
 * @param <E>: {@link MinaWritable} type to iterate over.
 */
public class MinaWritableIterable<W extends MinaWritable> implements Iterable<W> {

	public MessagesIterator<W> messagesIterator;

	public MinaWritableIterable() {
		this.messagesIterator = new MessagesIterator<W>();
	}

	@Override
	public Iterator<W> iterator() {
		reset();
		return this.messagesIterator;
	}

	public void reset() {
		this.messagesIterator.reset();;
	}

	public static class MessagesIterator<M extends MinaWritable> extends MinaWritableIterator<M>
		implements Iterator<M> {

		@Override
		public M next() {
			currentByteLocation += representativeWritableInstance.read(writableBytes,
				currentByteLocation);
			return representativeWritableInstance;
		}
	}
}