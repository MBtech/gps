package gps.graph;

import org.apache.commons.cli.CommandLine;

import gps.node.worker.AbstractGPSWorker;
import gps.writable.MinaWritable;

/**
 * Factory class that will be used by {@link AbstractGPSWorker} to instantiate new vertices. Should
 * be extended by the algorithm being implemented..
 * 
 * @author semihsalihoglu
 */
public abstract class VertexFactory<V extends MinaWritable, E extends MinaWritable,
	M extends MinaWritable> {

	public abstract Vertex<V, E, M> newInstance(CommandLine commandline);
}
