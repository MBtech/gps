package gps.graph;

import java.util.Iterator;

import gps.writable.MinaWritable;

/**
 * {@link java.lang.Iterable} implementation of iterating over a list of typed edges.
 * 
 * @author semihsalihoglu
 *
 * @param <E>: {@link MinaWritable} type to store edge values
 */
public class EdgeIterable<E extends MinaWritable> implements Iterable<Edge<E>> {

	public EdgeIterator<E> edgeIterator;

	public EdgeIterable() {
		this.edgeIterator = new EdgeIterator<E>();
	}

	@Override
	public Iterator<Edge<E>> iterator() {
		reset();
		return this.edgeIterator;
	}

	public void reset() {
		this.edgeIterator.reset();
	}
}
